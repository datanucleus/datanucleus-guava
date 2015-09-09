/**********************************************************************
Copyright (c) 2015 Renato Garcia and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors:
    ...
**********************************************************************/
package org.datanucleus.store.types.guava.containers;

import java.util.Arrays;

import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.store.types.containers.JDKCollectionHandler;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

public class MultisetHandler extends JDKCollectionHandler<Multiset>
{
    @Override
    public Multiset newContainer(AbstractMemberMetaData mmm)
    {
        return HashMultiset.create();
    }

    @Override
    public Multiset newContainer(AbstractMemberMetaData mmd, Object... objects)
    {
        return HashMultiset.create(Arrays.asList(objects));
    }
}
