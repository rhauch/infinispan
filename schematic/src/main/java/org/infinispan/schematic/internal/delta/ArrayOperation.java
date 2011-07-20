/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.schematic.internal.delta;

import net.jcip.annotations.Immutable;

import org.infinispan.schematic.document.Array;
import org.infinispan.schematic.document.Path;
import org.infinispan.schematic.internal.document.MutableArray;
import org.infinispan.schematic.internal.document.MutableDocument;

/**
 * An atomic operation on an {@link Array} for SchematicValueDelta.
 * <p/>
 * 
 * @author (various)
 * @since 5.1
 */
@Immutable
public abstract class ArrayOperation extends Operation {

   protected ArrayOperation(Path path) {
      super(path);
   }

   @Override
   public void replay(MutableDocument delegate) {
      replay((MutableArray) delegate);
   }

   @Override
   public void rollback(MutableDocument delegate) {
      rollback((MutableArray) delegate);
   }

   public abstract void replay(MutableArray delegate);

   public abstract void rollback(MutableArray delegate);

}
