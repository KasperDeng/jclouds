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
import org.jclouds.openstack.nova.v2_0.domain.Quota;
import org.jclouds.openstack.nova.v2_0.extensions.QuotaApi;

import javax.annotation.Resource;
import javax.inject.Named;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A function for getting quota
 *
 */
public class GetQuota
      implements Function<List<String>, Optional<Quota>> {

   @Resource
   @Named(ComputeServiceConstants.COMPUTE_LOGGER)
   protected Logger logger = Logger.NULL;

   private final NovaApi novaApi;

   @Inject
   public GetQuota(NovaApi novaApi) {
      this.novaApi = checkNotNull(novaApi, "novaApi");
   }

   /**
    * @param input A list of string if regionId and tenantId
    * @return quota info
    */
   @Override
   public Optional<Quota> apply(List<String> input) {
      checkArgument(input.size() >=2, "invalid arguments input to GetQuota");
      Optional<? extends QuotaApi> quotaApi = novaApi.getQuotaApi(input.get(0));
      if (quotaApi.isPresent()) {
         return Optional.of(quotaApi.get().getByTenant(input.get(1)));
      }
      return Optional.absent();
   }

   @Override
   public String toString() {
      return MoreObjects.toStringHelper("GetQuota").toString();
   }

}
