/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jclouds.openstack.nova.v2_0.compute;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import org.jclouds.Constants;
import org.jclouds.collect.Memoized;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.callables.RunScriptOnNode;
import org.jclouds.compute.domain.Hardware;
import org.jclouds.compute.domain.Image;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.TemplateBuilder;
import org.jclouds.compute.extensions.ImageExtension;
import org.jclouds.compute.extensions.SecurityGroupExtension;
import org.jclouds.compute.functions.GroupNamingConvention;
import org.jclouds.compute.internal.BaseComputeService;
import org.jclouds.compute.internal.PersistNodeCredentials;
import org.jclouds.compute.options.TemplateOptions;
import org.jclouds.compute.reference.ComputeServiceConstants.Timeouts;
import org.jclouds.compute.strategy.CreateNodesInGroupThenAddToSet;
import org.jclouds.compute.strategy.DestroyNodeStrategy;
import org.jclouds.compute.strategy.GetImageStrategy;
import org.jclouds.compute.strategy.GetNodeMetadataStrategy;
import org.jclouds.compute.strategy.InitializeRunScriptOnNodeOrPlaceInBadMap;
import org.jclouds.compute.strategy.ListNodesStrategy;
import org.jclouds.compute.strategy.RebootNodeStrategy;
import org.jclouds.compute.strategy.ResumeNodeStrategy;
import org.jclouds.compute.strategy.SuspendNodeStrategy;
import org.jclouds.domain.Credentials;
import org.jclouds.domain.Location;
import org.jclouds.openstack.nova.v2_0.NovaApi;
import org.jclouds.openstack.nova.v2_0.compute.options.NovaTemplateOptions;
import org.jclouds.openstack.nova.v2_0.domain.Flavor;
import org.jclouds.openstack.nova.v2_0.domain.KeyPair;
import org.jclouds.openstack.nova.v2_0.domain.Quota;
import org.jclouds.openstack.nova.v2_0.domain.SecurityGroup;
import org.jclouds.openstack.nova.v2_0.domain.SimpleServerUsage;
import org.jclouds.openstack.nova.v2_0.domain.SimpleTenantUsage;
import org.jclouds.openstack.nova.v2_0.domain.zonescoped.SecurityGroupInZone;
import org.jclouds.openstack.nova.v2_0.domain.zonescoped.ZoneAndId;
import org.jclouds.openstack.nova.v2_0.domain.zonescoped.ZoneAndName;
import org.jclouds.openstack.nova.v2_0.extensions.KeyPairApi;
import org.jclouds.openstack.nova.v2_0.extensions.QuotaApi;
import org.jclouds.openstack.nova.v2_0.extensions.SecurityGroupApi;
import org.jclouds.openstack.nova.v2_0.extensions.SimpleTenantUsageApi;
import org.jclouds.openstack.nova.v2_0.features.FlavorApi;
import org.jclouds.openstack.nova.v2_0.predicates.SecurityGroupPredicates;
import org.jclouds.scriptbuilder.functions.InitAdminAccess;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListeningExecutorService;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.jclouds.compute.config.ComputeServiceProperties.TIMEOUT_NODE_RUNNING;
import static org.jclouds.compute.config.ComputeServiceProperties.TIMEOUT_NODE_SUSPENDED;
import static org.jclouds.compute.config.ComputeServiceProperties.TIMEOUT_NODE_TERMINATED;
import static org.jclouds.openstack.nova.v2_0.predicates.KeyPairPredicates.nameMatches;
import static org.jclouds.util.Predicates2.retry;

@Singleton
public class NovaComputeService extends BaseComputeService {
   protected final NovaApi novaApi;
   protected final LoadingCache<ZoneAndName, SecurityGroupInZone> securityGroupMap;
   protected final LoadingCache<ZoneAndName, KeyPair> keyPairCache;
   protected final Function<Set<? extends NodeMetadata>, Multimap<String, String>> orphanedGroupsByZoneId;
   protected final GroupNamingConvention.Factory namingConvention;

   @Inject
   protected NovaComputeService(ComputeServiceContext context, Map<String, Credentials> credentialStore,
            @Memoized Supplier<Set<? extends Image>> images, @Memoized Supplier<Set<? extends Hardware>> sizes,
            @Memoized Supplier<Set<? extends Location>> locations, ListNodesStrategy listNodesStrategy,
            GetImageStrategy getImageStrategy, GetNodeMetadataStrategy getNodeMetadataStrategy,
            CreateNodesInGroupThenAddToSet runNodesAndAddToSetStrategy, RebootNodeStrategy rebootNodeStrategy,
            DestroyNodeStrategy destroyNodeStrategy, ResumeNodeStrategy startNodeStrategy,
            SuspendNodeStrategy stopNodeStrategy, Provider<TemplateBuilder> templateBuilderProvider,
            @Named("DEFAULT") Provider<TemplateOptions> templateOptionsProvider,
            @Named(TIMEOUT_NODE_RUNNING) Predicate<AtomicReference<NodeMetadata>> nodeRunning,
            @Named(TIMEOUT_NODE_TERMINATED) Predicate<AtomicReference<NodeMetadata>> nodeTerminated,
            @Named(TIMEOUT_NODE_SUSPENDED) Predicate<AtomicReference<NodeMetadata>> nodeSuspended,
            InitializeRunScriptOnNodeOrPlaceInBadMap.Factory initScriptRunnerFactory,
            RunScriptOnNode.Factory runScriptOnNodeFactory, InitAdminAccess initAdminAccess,
            PersistNodeCredentials persistNodeCredentials, Timeouts timeouts,
            @Named(Constants.PROPERTY_USER_THREADS) ListeningExecutorService userExecutor, NovaApi novaApi,
            LoadingCache<ZoneAndName, SecurityGroupInZone> securityGroupMap,
            LoadingCache<ZoneAndName, KeyPair> keyPairCache,
            Function<Set<? extends NodeMetadata>, Multimap<String, String>> orphanedGroupsByZoneId,
            GroupNamingConvention.Factory namingConvention, Optional<ImageExtension> imageExtension,
            Optional<SecurityGroupExtension> securityGroupExtension) {
      super(context, credentialStore, images, sizes, locations, listNodesStrategy, getImageStrategy,
               getNodeMetadataStrategy, runNodesAndAddToSetStrategy, rebootNodeStrategy, destroyNodeStrategy,
               startNodeStrategy, stopNodeStrategy, templateBuilderProvider, templateOptionsProvider, nodeRunning,
               nodeTerminated, nodeSuspended, initScriptRunnerFactory, initAdminAccess, runScriptOnNodeFactory,
               persistNodeCredentials, timeouts, userExecutor, imageExtension, securityGroupExtension);
      this.novaApi = checkNotNull(novaApi, "novaApi");
      this.securityGroupMap = checkNotNull(securityGroupMap, "securityGroupMap");
      this.keyPairCache = checkNotNull(keyPairCache, "keyPairCache");
      this.orphanedGroupsByZoneId = checkNotNull(orphanedGroupsByZoneId, "orphanedGroupsByZoneId");
      this.namingConvention = checkNotNull(namingConvention, "namingConvention");
   }

   @Override
   protected void cleanUpIncidentalResourcesOfDeadNodes(Set<? extends NodeMetadata> deadNodes) {
      Multimap<String, String> zoneToZoneAndGroupNames = orphanedGroupsByZoneId.apply(deadNodes);
      for (Map.Entry<String, Collection<String>> entry : zoneToZoneAndGroupNames.asMap().entrySet()) {
         cleanOrphanedGroupsInZone(ImmutableSet.copyOf(entry.getValue()), entry.getKey());
      }
   }

   protected void cleanOrphanedGroupsInZone(Set<String> groups, String zoneId) {
      cleanupOrphanedSecurityGroupsInZone(groups, zoneId);
      cleanupOrphanedKeyPairsInZone(groups, zoneId);
   }

   private void cleanupOrphanedSecurityGroupsInZone(Set<String> groups, String zoneId) {
      Optional<? extends SecurityGroupApi> securityGroupApi = novaApi.getSecurityGroupExtensionForZone(zoneId);
      if (securityGroupApi.isPresent()) {
         for (String group : groups) {
            for (SecurityGroup securityGroup : Iterables.filter(securityGroupApi.get().list(),
                    SecurityGroupPredicates.nameMatches(namingConvention.create().containsGroup(group)))) {
               ZoneAndName zoneAndName = ZoneAndName.fromZoneAndName(zoneId, securityGroup.getName());
               logger.debug(">> deleting securityGroup(%s)", zoneAndName);
               securityGroupApi.get().delete(securityGroup.getId());
               // TODO: test this clear happens
               securityGroupMap.invalidate(zoneAndName);
               logger.debug("<< deleted securityGroup(%s)", zoneAndName);
            }
         }
      }
   }

   private void cleanupOrphanedKeyPairsInZone(Set<String> groups, String zoneId) {
      Optional<? extends KeyPairApi> keyPairApi = novaApi.getKeyPairExtensionForZone(zoneId);
      if (keyPairApi.isPresent()) {
         for (String group : groups) {
            for (KeyPair pair : keyPairApi.get().list().filter(nameMatches(namingConvention.create().containsGroup(group)))) {
               ZoneAndName zoneAndName = ZoneAndName.fromZoneAndName(zoneId, pair.getName());
               logger.debug(">> deleting keypair(%s)", zoneAndName);
               keyPairApi.get().delete(pair.getName());
               // TODO: test this clear happens
               keyPairCache.invalidate(zoneAndName);
               logger.debug("<< deleted keypair(%s)", zoneAndName);
            }
            keyPairCache.invalidate(ZoneAndName.fromZoneAndName(zoneId,
                     namingConvention.create().sharedNameForGroup(group)));
         }
      }
   }

   /**
    * returns template options, except of type {@link NovaTemplateOptions}.
    */
   @Override
   public NovaTemplateOptions templateOptions() {
      return NovaTemplateOptions.class.cast(super.templateOptions());
   }

    /**
     * rename the node
     * Introduced only for nova computer service, not implemented for others inherited from ComputeService yet
     */
    @Override
    public boolean renameNode(final String id, final String newName) {
        checkNotNull(id, "id");
        checkNotNull(newName, "newName");
        logger.debug(">> renaming node(%s)", id);
        Predicate<String> tester = retry(new Predicate<String>() {
            public boolean apply(String input) {
                try {
                    ZoneAndId zoneAndId = ZoneAndId.fromSlashEncoded(id);
                    novaApi.getServerApiForZone(zoneAndId.getZone()).rename(zoneAndId.getId(), newName);
                    return true;
                } catch (IllegalStateException e) {
                    logger.warn("<< illegal state renaming node(%s)", id);
                    return false;
                }
            }
        }, 10 * 1000, 1000, TimeUnit.MILLISECONDS);

        boolean result = tester.apply(id);
        logger.debug("<< renamed node(%s) result(%s)", id, result);
        return result;
    }

    /**
     * get tenant quota
     * Introduced only for nova computer service, not implemented for others inherited from ComputeService yet
     */
    @Override
    public Map<String, Integer> getQuotaByTenant(String... args) {
        String zone = args[0];
        String tenant = args[1];
        Map<String, Integer> hardwareMap = new HashMap<String, Integer>();
        Optional<? extends QuotaApi> quotaApi = novaApi.getQuotaExtensionForZone(zone);
        if (quotaApi.isPresent()) {
            Quota quota = quotaApi.get().getByTenant(tenant);
            hardwareMap.put("vcpu", quota.getCores());
            hardwareMap.put("ram", quota.getRam());
            hardwareMap.put("disk", quota.getGigabytes());
            hardwareMap.put("floatingIp", quota.getFloatingIps());
            hardwareMap.put("instance", quota.getInstances());
        }
        return hardwareMap;
    }

    /**
     * get flavor by flavor Id
     * Introduced only for nova computer service, not implemented for others inherited from ComputeService yet
     */
    @Override
    public Map<String, Integer> getFlavorByFlavorId(String... args) {
        String zone = args[0];
        String flavorId = args[1];
        Map<String, Integer> hardwareMap = new HashMap<String, Integer>();
        FlavorApi flavorApi = novaApi.getFlavorApiForZone(zone);
        if (flavorApi != null) {
            Flavor flavor = flavorApi.get(flavorId);
            if (flavor != null) {
                hardwareMap.put("vcpu", flavor.getVcpus());
                hardwareMap.put("ram", flavor.getRam());
                hardwareMap.put("disk", flavor.getDisk());
            }
        }
        return hardwareMap;
    }

    /**
     * get tenant usage by calling the simple tenant usage API
     * Introduced only for nova computer service, not implemented for others inherited from ComputeService yet
     */
    @Override
    public Map<String, Integer> getTotalUsageByTenant(String... args) {
        String zone = args[0];
        String tenantId = args[1];
        Map<String, Integer> totalUsageMap = new HashMap<String, Integer>();
        Optional<? extends SimpleTenantUsageApi> simpleTenantUsageApi = novaApi.getSimpleTenantUsageExtensionForZone(zone);
        if (simpleTenantUsageApi.isPresent()) {
            SimpleTenantUsage simpleTenantUsage = simpleTenantUsageApi.get().get(tenantId);
            Set<SimpleServerUsage> simpleServerUsageHashSet = simpleTenantUsage.getServerUsages();
            int vcpuUsage = 0;
            int ramUsage = 0;
            int diskUsage = 0;
            int instanceUsage = simpleServerUsageHashSet.size();
            for (SimpleServerUsage usage : simpleServerUsageHashSet) {
                vcpuUsage += usage.getFlavorVcpus();
                ramUsage += usage.getFlavorMemoryMb();
                diskUsage += usage.getFlavorLocalGb();
            }

            totalUsageMap.put("vcpu", vcpuUsage);
            totalUsageMap.put("ram", ramUsage);
            totalUsageMap.put("disk", diskUsage);
            totalUsageMap.put("instance", instanceUsage);
        }
        return totalUsageMap;
    }

    public Quota getQuotaByTenant(String zone, String tenant) {
        Optional<? extends QuotaApi> quotaApi = novaApi.getQuotaExtensionForZone(zone);
        if (quotaApi.isPresent()) {
            return quotaApi.get().getByTenant(tenant);
        }
        return null;
    }

    public Flavor getFlavorByFlavorId(String zone, String flavorId) {
        FlavorApi flavorApi = novaApi.getFlavorApiForZone(zone);
        if (flavorApi != null) {
            return flavorApi.get(flavorId);
        }
        return null;
    }

}
