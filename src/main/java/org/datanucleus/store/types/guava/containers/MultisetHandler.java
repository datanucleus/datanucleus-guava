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
