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
package org.jclouds.openstack.nova.v2_0.compute.functions.ext;

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.inject.Inject;
import org.jclouds.compute.reference.ComputeServiceConstants;
import org.jclouds.logging.Logger;
import org.jclouds.openstack.nova.v2_0.NovaApi;
import org.jclouds.openstack.nova.v2_0.domain.SimpleTenantUsage;
import org.jclouds.openstack.nova.v2_0.extensions.SimpleTenantUsageApi;

import javax.annotation.Resource;
import javax.inject.Named;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A function for getting tenant usage by calling the simple tenant usage API
 */
public class GetTenantUsage implements Function<List<String>, Optional<SimpleTenantUsage>> {

    @Resource
    @Named(ComputeServiceConstants.COMPUTE_LOGGER)
    protected Logger logger = Logger.NULL;

    private final NovaApi novaApi;

    @Inject
    public GetTenantUsage(NovaApi novaApi) {
        this.novaApi = checkNotNull(novaApi, "novaApi");
    }

    /**
     * @param input A list of string if regionId and tenantId
     * @return Tenant usage
     */
    @Override
    public Optional<SimpleTenantUsage> apply(List<String> input) {
        checkArgument(input.size() >= 2, "invalid arguments input to GetTenantUsage");
        Optional<? extends SimpleTenantUsageApi> simpleTenantUsageApi =
            novaApi.getSimpleTenantUsageApi(input.get(0));
        if (simpleTenantUsageApi.isPresent()) {
            SimpleTenantUsage simpleTenantUsage = simpleTenantUsageApi.get().get(input.get(1));
            if (simpleTenantUsage != null) {
                return Optional.of(simpleTenantUsage);
            }
        }
        return Optional.absent();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper("GetTenantUsage").toString();
    }

}
