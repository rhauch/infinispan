/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.cdi.test.cache.cachemanager;

import org.infinispan.cdi.Infinispan;
import org.infinispan.config.Configuration;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import static org.infinispan.eviction.EvictionStrategy.FIFO;

/**
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public class Config {
   /**
    * Associates the "large" cache with the qualifier {@link Large}.
    */
   @Large
   @Infinispan("large")
   @Produces
   Configuration largeConfiguration() {
      return new Configuration().fluent()
            .eviction().maxEntries(2000)
            .build();
   }

   /**
    * Associates the "small" cache with the qualifier {@link Small}.
    */
   @Small
   @Infinispan("small")
   @Produces
   Configuration smallConfiguration() {
      return new Configuration().fluent()
            .eviction().maxEntries(20)
            .build();
   }

   /**
    * Associates the "small" and "large" caches with this specific cache manager.
    */
   @Large
   @Small
   @Produces
   @ApplicationScoped
   EmbeddedCacheManager specificCacheManager() {
      return new DefaultCacheManager(new Configuration().fluent()
                                           .eviction().strategy(FIFO)
                                           .build());
   }
}
