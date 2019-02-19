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
import com.google.common.base.Predicate;
import com.google.inject.Inject;
import org.jclouds.compute.reference.ComputeServiceConstants;
import org.jclouds.logging.Logger;
import org.jclouds.openstack.nova.v2_0.NovaApi;
import org.jclouds.openstack.nova.v2_0.domain.regionscoped.RegionAndId;

import javax.annotation.Resource;
import javax.inject.Named;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.jclouds.util.Predicates2.retry;

/**
 * A function for renaming the node
 */
public class RenameNode implements Function<List<String>, Boolean> {

    @Resource
    @Named(ComputeServiceConstants.COMPUTE_LOGGER)
    protected Logger logger = Logger.NULL;

    private final NovaApi novaApi;

    @Inject
    public RenameNode(NovaApi novaApi) {
        this.novaApi = checkNotNull(novaApi, "novaApi");
    }

    /**
     * @param input A list of string if nodeId and newNodeName
     * @return TRUE rename node successfully, otherwise FALSE
     */
    @Override
    public Boolean apply(List<String> input) {
        checkArgument(input.size() >=2, "invalid arguments input to RenameNode");
        final String id = input.get(0);
        checkNotNull(id, "id");
        final String newName = input.get(1);
        checkNotNull(newName, "newName");
        logger.debug(">> renaming node(%s)", id);
        Predicate<String> tester = retry(new Predicate<String>() {
            public boolean apply(String input) {
                try {
                    RegionAndId regionAndId = RegionAndId.fromSlashEncoded(id);
                    novaApi.getServerApiForZone(regionAndId.getRegion())
                        .rename(regionAndId.getId(), newName);
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

    @Override
    public String toString() {
        return MoreObjects.toStringHelper("RenameNode").toString();
    }

}
