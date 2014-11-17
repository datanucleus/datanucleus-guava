/**********************************************************************
Copyright (c) 2010 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.store.types.guava.wrappers;

import java.io.ObjectStreamException;
import java.util.Collection;
import java.util.Iterator;

import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.state.FetchPlanState;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.state.RelationshipManager;
import org.datanucleus.store.types.SCOCollection;
import org.datanucleus.store.types.SCOCollectionIterator;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

import com.google.common.collect.ForwardingMultiset;
import com.google.common.collect.HashMultiset;

/**
 * A mutable second-class MultiSet object.
 * This is the simplified form that intercepts mutators and marks the field as dirty.
 * Note that we cannot explicitly support HashMultiset etc since Google made these final.
 */
public class Multiset<E> extends ForwardingMultiset<E> implements SCOCollection<com.google.common.collect.Multiset>, Cloneable
{
    protected transient ObjectProvider ownerOP;
    protected transient AbstractMemberMetaData ownerMmd;

    /** The internal "delegate". */
    protected HashMultiset<E> delegate;

    /**
     * Constructor, using the ObjectProvider of the "owner" and the member.
     * @param op The owner ObjectProvider
     * @param mmd Metadata for the member.
     */
    public Multiset(ObjectProvider op, AbstractMemberMetaData mmd)
    {
        this.ownerOP = op;
        this.ownerMmd = mmd;
    }

    /**
     * Method to initialise the SCO from an existing value.
     * @param c The object to set from
     * @param forInsert Whether the object needs inserting in the datastore with this value
     * @param forUpdate Whether to update the datastore with this value
     */
    public void initialise(com.google.common.collect.Multiset c, boolean forInsert, boolean forUpdate)
    {
        delegate = HashMultiset.create();
        if (c != null)
        {
            delegate.addAll(c); // Make copy of the elements rather than using same memory
        }
        if (NucleusLogger.PERSISTENCE.isDebugEnabled())
        {
            NucleusLogger.PERSISTENCE.debug(Localiser.msg("023003", this.getClass().getName(), ownerOP.getObjectAsPrintable(), ownerMmd.getName(), "" + size(), 
                SCOUtils.getSCOWrapperOptionsMessage(true, false, true, false)));
        }
    }

    /**
     * Method to initialise the SCO for use.
     */
    public void initialise()
    {
        delegate = HashMultiset.create();
        if (NucleusLogger.PERSISTENCE.isDebugEnabled())
        {
            NucleusLogger.PERSISTENCE.debug(Localiser.msg("023003", this.getClass().getName(), ownerOP.getObjectAsPrintable(), ownerMmd.getName(), "" + size(), 
                SCOUtils.getSCOWrapperOptionsMessage(true, false, true, false)));
        }
    }

    // ----------------------- Implementation of SCO methods -------------------

    /**
     * Accessor for the unwrapped value that we are wrapping.
     * @return The unwrapped value
     */
    public com.google.common.collect.Multiset getValue()
    {
        return delegate;
    }

    /**
     * Method to effect the load of the data in the SCO.
     * Used when the SCO supports lazy-loading to tell it to load all now.
     */
    public void load()
    {
        // Always loaded
    }

    /**
     * Method to return if the SCO has its contents loaded. Returns true.
     * @return Whether it is loaded
     */
    public boolean isLoaded()
    {
        return true;
    }

    /**
     * Method to update an embedded element in this collection.
     * @param element The element
     * @param fieldNumber Number of field in the element
     * @param value New value for this field
     */
    public void updateEmbeddedElement(Object element, int fieldNumber, Object value)
    {
        // Just mark field in embedded owners as dirty
        makeDirty();
    }

    /**
     * Accessor for the field name.
     * @return The field name
     */
    public String getFieldName()
    {
        return ownerMmd.getName();
    }

    /**
     * Accessor for the owner object.
     * @return The owner object
     */
    public Object getOwner()
    {
        return (ownerOP != null ? ownerOP.getObject() : null);
    }

    /**
     * Method to unset the owner and field information.
     */
    public synchronized void unsetOwner()
    {
        if (ownerOP != null)
        {
            ownerOP = null;
            ownerMmd = null;
        }
    }

    /**
     * Utility to mark the object as dirty
     **/
    public void makeDirty()
    {
        if (ownerOP != null)
        {
            ownerOP.makeDirty(ownerMmd.getAbsoluteFieldNumber());
        }
    }

    /**
     * Method to return a detached copy of the container.
     * Recurse sthrough the elements so that they are likewise detached.
     * @param state State for detachment process
     * @return The detached container
     */
    public com.google.common.collect.Multiset detachCopy(FetchPlanState state)
    {
        com.google.common.collect.Multiset detached = HashMultiset.create();
        SCOUtils.detachCopyForCollection(ownerOP, toArray(), state, detached);
        return detached;
    }

    /**
     * Method to return an attached copy of the passed (detached) value. The returned attached copy
     * is a SCO wrapper. Goes through the existing elements in the store for this owner field and
     * removes ones no longer present, and adds new elements. All elements in the (detached)
     * value are attached.
     * @param value The new (collection) value
     */
    public void attachCopy(com.google.common.collect.Multiset value)
    {
        boolean elementsWithoutIdentity = SCOUtils.collectionHasElementsWithoutIdentity(ownerMmd);
        SCOUtils.attachCopyElements(ownerOP, this, value, elementsWithoutIdentity);

/*        // Remove any no-longer-needed elements from this collection
        SCOUtils.attachRemoveDeletedElements(ownerOP.getExecutionContext().getApiAdapter(), this, c, elementsWithoutIdentity);

        // Persist any new elements and form the attached elements collection
        java.util.Collection attachedElements = new java.util.HashSet(c.size());
        SCOUtils.attachCopyForCollection(ownerOP, c.toArray(), attachedElements, elementsWithoutIdentity);

        // Add any new elements to this collection
        SCOUtils.attachAddNewElements(ownerOP.getExecutionContext().getApiAdapter(), this, attachedElements,
            elementsWithoutIdentity);*/
    }

    // ------------------ Implementation of methods --------------------

    /**
     * Creates and returns a copy of this object.
     * @return The cloned object
     */
    public Object clone()
    {
        // TODO Implement clone()
        return null;
    }

    /**
     * Accessor for an iterator for the Set.
     * @return The iterator
     */
    public Iterator iterator()
    {
        return new SCOCollectionIterator(this, ownerOP, delegate, null, true);
    }

    /**
     * Method to add an element to the HashSet.
     * @param element The new element
     * @return Whether it was added ok.
     */
    public boolean add(E element)
    {
        boolean success = delegate.add(element);
        if (ownerOP != null && ownerOP.getExecutionContext().getManageRelations())
        {
            // Relationship management
            ownerOP.getExecutionContext().getRelationshipManager(ownerOP).relationAdd(ownerMmd.getAbsoluteFieldNumber(), element);
        }
        if (success)
        {
            makeDirty();
        }
        return success;
    }

    /**
     * Method to add occurrences of an element to the HashSet.
     * @param element The new element
     * @param num Number of occurrences to add
     * @return Count of the element before
     */
    public int add(E element, int num)
    {
        if (num < 0)
        {
            throw new IllegalArgumentException("Number of occurrences is negative");
        }
        int origNum = delegate.add(element, num);
        if (ownerOP != null && ownerOP.getExecutionContext().getManageRelations())
        {
            // Relationship management
            ownerOP.getExecutionContext().getRelationshipManager(ownerOP).relationAdd(ownerMmd.getAbsoluteFieldNumber(), element);
        }
        if (num > 0)
        {
            makeDirty();
        }
        return origNum;
    }

    /**
     * Method to add a collection to the HashSet.
     * @param c The collection
     * @return Whether it was added ok.
     */
    public boolean addAll(Collection c)
    {
        boolean success = delegate.addAll(c);
        if (ownerOP != null && ownerOP.getExecutionContext().getManageRelations())
        {
            // Relationship management
            Iterator iter = c.iterator();
            RelationshipManager relMgr = ownerOP.getExecutionContext().getRelationshipManager(ownerOP);
            while (iter.hasNext())
            {
                relMgr.relationAdd(ownerMmd.getAbsoluteFieldNumber(), iter.next());
            }
        }
        if (success)
        {
            makeDirty();
        }
        return success;
    }

    /**
     * Method to clear the HashSet
     */
    public void clear()
    {
        delegate.clear();
        makeDirty();
    }

    /**
     * Method to remove an element from the set.
     * @param element The Element to remove
     * @return Whether it was removed successfully.
     */
    public synchronized boolean remove(Object element)
    {
        return remove(element, true);
    }

    /**
     * Method to remove occurrences of an element from the set.
     * @param element The Element to remove
     * @param num Number of occurrences
     * @return Number of occurrences before
     */
    public synchronized int remove(Object element, int num)
    {
        if (num < 0)
        {
            throw new IllegalArgumentException("Number of occurrences is negative");
        }
        int numOrig = delegate.remove(element, num);
        if (ownerOP != null && ownerOP.getExecutionContext().getManageRelations())
        {
            ownerOP.getExecutionContext().getRelationshipManager(ownerOP).relationRemove(ownerMmd.getAbsoluteFieldNumber(), element);
        }
        if (num > 0)
        {
            makeDirty();
        }
        return numOrig;
    }

    /**
     * Method to remove an element from the List
     * @param element The Element to remove
     * @return Whether it was removed successfully.
     */
    public synchronized boolean remove(Object element, boolean allowCascadeDelete)
    {
        boolean success = delegate.remove(element);
        if (ownerOP != null && ownerOP.getExecutionContext().getManageRelations())
        {
            ownerOP.getExecutionContext().getRelationshipManager(ownerOP).relationRemove(ownerMmd.getAbsoluteFieldNumber(), element);
        }
        if (success)
        {
            makeDirty();
        }
        return success;
    }

    /**
     * Method to remove all elements from the collection from the HashSet.
     * @param c The collection of elements to remove 
     * @return Whether it was removed ok.
     */
    public boolean removeAll(java.util.Collection c)
    {
        boolean success = delegate.removeAll(c);
        if (ownerOP != null && ownerOP.getExecutionContext().getManageRelations())
        {
            // Relationship management
            Iterator iter = c.iterator();
            RelationshipManager relMgr = ownerOP.getExecutionContext().getRelationshipManager(ownerOP);
            while (iter.hasNext())
            {
                relMgr.relationRemove(ownerMmd.getAbsoluteFieldNumber(), iter.next());
            }
        }
        if (success)
        {
            makeDirty();
        }
        return success;
    }

    /**
     * Method to retain a Collection of elements (and remove all others).
     * @param c The collection to retain
     * @return Whether they were retained successfully.
     */
    public synchronized boolean retainAll(java.util.Collection c)
    {
        boolean success = delegate.retainAll(c);
        if (success)
        {
            makeDirty();
        }
        return success;
    }

    /**
     * Add or remove occurrences of the element so it has the specified count.
     * @param elem The element
     * @param num The number required
     * @return The number of occurrences before
     */
    public int setCount(E elem, int num)
    {
        if (num < 0)
        {
            throw new IllegalArgumentException("Number of occurrences is negative");
        }
        int origNum = delegate.count(elem);
        if (origNum < num)
        {
            add(elem, num-origNum);
        }
        else if (origNum > num)
        {
            remove(elem, origNum-num);
        }
        return origNum;
    }

    /**
     * The writeReplace method is called when ObjectOutputStream is preparing
     * to write the object to the stream. The ObjectOutputStream checks
     * whether the class defines the writeReplace method. If the method is
     * defined, the writeReplace method is called to allow the object to
     * designate its replacement in the stream. The object returned should be
     * either of the same type as the object passed in or an object that when
     * read and resolved will result in an object of a type that is compatible
     * with all references to the object.
     * @return the replaced object
     * @throws ObjectStreamException if an error occurs
     */
    protected Object writeReplace() throws ObjectStreamException
    {
        return new java.util.HashSet(delegate);
    }

    @Override
    protected com.google.common.collect.Multiset delegate()
    {
        return delegate;
    }
}