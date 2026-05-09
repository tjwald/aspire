// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace Aspire.Hosting.RabbitMQ.Provisioning;

/// <summary>
/// Shared <see cref="IHealthCheck"/> implementation for all RabbitMQ child resources (virtual hosts, queues, exchanges, shovels, policies).
/// </summary>
/// <remarks>
/// The check proceeds in three stages: returns <see cref="HealthStatus.Unhealthy"/> if provisioning has not started or failed,
/// then checks each <see cref="IRabbitMQProvisionable.HealthDependencies"/> task the same way, and finally calls
/// <see cref="IRabbitMQProvisionable.ProbeAsync"/> for a live broker verification.
/// </remarks>
internal sealed class RabbitMQProvisionableHealthCheck(IRabbitMQProvisionable self, IRabbitMQProvisioningClient client, ILogger<RabbitMQProvisionableHealthCheck> logger) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        // Stage 1: own provisioning — read task state synchronously (no blocking wait)
        if (!self.ProvisionedTask.IsCompleted)
        {
            return HealthCheckResult.Unhealthy($"Provisioning of '{self.Name}' has not started yet.");
        }

        if (self.ProvisionedTask.IsFaulted)
        {
            var ex = self.ProvisionedTask.Exception?.InnerException ?? self.ProvisionedTask.Exception;
            var message = $"Provisioning of '{self.Name}' failed: {ex?.Message}";
            logger.LogWarning(ex, "{Message}", message);
            return HealthCheckResult.Unhealthy(message, ex);
        }

        // Stage 2: health dependencies (e.g. policies that apply to this queue/exchange)
        foreach (var dep in self.HealthDependencies)
        {
            if (!dep.ProvisionedTask.IsCompleted)
            {
                return HealthCheckResult.Unhealthy($"Dependent resource '{dep.Name}' has not started provisioning yet.");
            }

            if (dep.ProvisionedTask.IsFaulted)
            {
                var ex = dep.ProvisionedTask.Exception?.InnerException ?? dep.ProvisionedTask.Exception;
                var message = $"Dependent resource '{dep.Name}' failed to provision: {ex?.Message}";
                logger.LogWarning(ex, "{Message}", message);
                return HealthCheckResult.Unhealthy(message, ex);
            }
        }

        // Stage 3: live broker probe
        var probe = await self.ProbeAsync(client, cancellationToken).ConfigureAwait(false);
        if (!probe.IsHealthy)
        {
            logger.LogWarning("Health probe for '{Resource}' failed: {Reason}", self.Name, probe.Description);
            return HealthCheckResult.Unhealthy(probe.Description);
        }

        return HealthCheckResult.Healthy();
    }
}
