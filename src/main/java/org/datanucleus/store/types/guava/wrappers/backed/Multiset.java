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
package org.datanucleus.store.types.guava.wrappers.backed;

import java.io.ObjectStreamException;
import java.util.Collection;
import java.util.Iterator;

import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.flush.CollectionAddOperation;
import org.datanucleus.flush.CollectionClearOperation;
import org.datanucleus.flush.CollectionRemoveOperation;
import org.datanucleus.flush.Operation;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.FieldPersistenceModifier;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.state.RelationshipManager;
import org.datanucleus.store.BackedSCOStoreManager;
import org.datanucleus.store.types.SCOCollectionIterator;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.store.types.scostore.CollectionStore;
import org.datanucleus.store.types.scostore.Store;
import org.datanucleus.store.types.wrappers.backed.BackedSCO;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

import com.google.common.collect.HashMultiset;

/**
 * A mutable second-class MultiSet object.
 * This class extends MultiSet, using that class to contain the current objects, and the backing store 
 * to be the interface to the datastore. A "backing store" is not present for datastores that dont use
 * DatastoreClass, or if the container is serialised or non-persistent.
 * 
 * <H3>Modes of Operation</H3>
 * The user can operate the list in 2 modes.
 * The <B>cached</B> mode will use an internal cache of the elements (in the "delegate") reading them at
 * the first opportunity and then using the cache thereafter.
 * The <B>non-cached</B> mode will just go direct to the "backing store" each call.
 *
 * <H3>Mutators</H3>
 * When the "backing store" is present any updates are passed direct to the datastore as well as to the "delegate".
 * If the "backing store" isn't present the changes are made to the "delegate" only.
 *
 * <H3>Accessors</H3>
 * When any accessor method is invoked, it typically checks whether the container has been loaded from its
 * "backing store" (where present) and does this as necessary. Some methods (<B>size()</B>) just check if 
 * everything is loaded and use the delegate if possible, otherwise going direct to the datastore.
 */
public class Multiset<E> extends org.datanucleus.store.types.guava.wrappers.Multiset<E> implements BackedSCO
{
    protected transient CollectionStore<E> backingStore;
    protected transient boolean allowNulls = false;
    protected transient boolean useCache = true;
    protected transient boolean isCacheLoaded = false;

    /**
     * Constructor, using StateManager of the "owner" and the field name.
     * @param sm The owner StateManager
     * @param mmd Metadata for the member
     */
    public Multiset(DNStateManager sm, AbstractMemberMetaData mmd)
    {
        super(sm, mmd);

        // Set up our delegate
        this.delegate = HashMultiset.create();
        this.allowNulls = SCOUtils.allowNullsInContainer(allowNulls, ownerMmd);
        this.useCache = SCOUtils.useContainerCache(sm, ownerMmd);

        if (!SCOUtils.collectionHasSerialisedElements(ownerMmd) && ownerMmd.getPersistenceModifier() == FieldPersistenceModifier.PERSISTENT)
        {
            this.backingStore = (CollectionStore)((BackedSCOStoreManager)ownerSM.getStoreManager()).getBackingStoreForField(sm.getExecutionContext().getClassLoaderResolver(), 
                mmd, java.util.HashSet.class);
        }

        if (NucleusLogger.PERSISTENCE.isDebugEnabled())
        {
            NucleusLogger.PERSISTENCE.debug(SCOUtils.getContainerInfoMessage(sm, ownerMmd.getName(), this, useCache, allowNulls, SCOUtils.useCachedLazyLoading(sm, ownerMmd)));
        }
    }

    public void initialise(com.google.common.collect.Multiset<E> newValue, Object oldValue)
    {
        if (newValue != null)
        {
            // Check for the case of serialised PC elements, and assign StateManagers to the elements without
            if (SCOUtils.collectionHasSerialisedElements(ownerMmd) && ownerMmd.getCollection().elementIsPersistent())
            {
                ExecutionContext ec = ownerSM.getExecutionContext();
                Iterator iter = newValue.iterator();
                while (iter.hasNext())
                {
                    Object pc = iter.next();
                    DNStateManager objSM = ec.findStateManager(pc);
                    if (objSM == null)
                    {
                        objSM = ec.getNucleusContext().getStateManagerFactory().newForEmbedded(ec, pc, false, ownerSM, ownerMmd.getAbsoluteFieldNumber(), null);
                    }
                }
            }

            if (backingStore != null && useCache && !isCacheLoaded)
            {
                // Mark as loaded
                isCacheLoaded = true;
            }

            if (NucleusLogger.PERSISTENCE.isDebugEnabled())
            {
                NucleusLogger.PERSISTENCE.debug(Localiser.msg("023008", ownerSM.getObjectAsPrintable(), ownerMmd.getName(), "" + newValue.size()));
            }

            // Detect which objects are added and which are deleted
            if (useCache)
            {
                Collection oldColl = (Collection)oldValue;
                if (oldColl != null)
                {
                    delegate.addAll(oldColl);
                }
                isCacheLoaded = true;

                SCOUtils.updateCollectionWithCollection(ownerSM.getExecutionContext().getApiAdapter(), this, newValue);
            }
            else
            {
                for (Object elem : newValue)
                {
                    if (!contains(elem))
                    {
                        add((E) elem);
                    }
                }
                Iterator iter = iterator();
                while (iter.hasNext())
                {
                    Object elem = iter.next();
                    if (!newValue.contains(elem))
                    {
                        remove(elem);
                    }
                }
            }
        }
    }

    /**
     * Method to initialise the SCO from an existing value.
     * @param c The object to set from
     */
    public void initialise(com.google.common.collect.Multiset<E> c)
    {
        if (c != null)
        {
            // Check for the case of serialised PC elements, and assign StateManagers to the elements without
            if (SCOUtils.collectionHasSerialisedElements(ownerMmd) && ownerMmd.getCollection().elementIsPersistent())
            {
                ExecutionContext ec = ownerSM.getExecutionContext();
                Iterator<E> iter = c.iterator();
                while (iter.hasNext())
                {
                    E pc = iter.next();
                    DNStateManager objSM = ec.findStateManager(pc);
                    if (objSM == null)
                    {
                        objSM = ec.getNucleusContext().getStateManagerFactory().newForEmbedded(ec, pc, false, ownerSM, ownerMmd.getAbsoluteFieldNumber(), null);
                    }
                }
            }

            if (NucleusLogger.PERSISTENCE.isDebugEnabled())
            {
                NucleusLogger.PERSISTENCE.debug(Localiser.msg("023007", ownerSM.getObjectAsPrintable(), ownerMmd.getName(), "" + c.size()));
            }

            delegate.addAll(c);
            isCacheLoaded = true;
        }
    }

    /**
     * Method to initialise the SCO for use.
     */
    public void initialise()
    {
        if (useCache && !SCOUtils.useCachedLazyLoading(ownerSM, ownerMmd))
        {
            // Load up the container now if not using lazy loading
            loadFromStore();
        }
    }

    // ----------------------- Implementation of SCO methods -------------------

    /**
     * Accessor for the unwrapped value that we are wrapping.
     * @return The unwrapped value
     */
    public com.google.common.collect.Multiset<E> getValue()
    {
        loadFromStore();
        return super.getValue();
    }

    /**
     * Method to effect the load of the data in the SCO.
     * Used when the SCO supports lazy-loading to tell it to load all now.
     */
    public void load()
    {
        if (useCache)
        {
            loadFromStore();
        }
    }

    /**
     * Method to return if the SCO has its contents loaded.
     * If the SCO doesn't support lazy loading will just return true.
     * @return Whether it is loaded
     */
    public boolean isLoaded()
    {
        return useCache ? isCacheLoaded : false;
    }

    /**
     * Method to load all elements from the "backing store" where appropriate.
     */
    protected void loadFromStore()
    {
        if (backingStore != null && !isCacheLoaded)
        {
            if (NucleusLogger.PERSISTENCE.isDebugEnabled())
            {
                NucleusLogger.PERSISTENCE.debug(Localiser.msg("023006", 
                    ownerSM.getObjectAsPrintable(), ownerMmd.getName()));
            }
            delegate.clear();
            Iterator<? extends E> iter = backingStore.iterator(ownerSM);
            while (iter.hasNext())
            {
                delegate.add(iter.next());
            }

            isCacheLoaded = true;
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.types.backed.BackedSCO#getBackingStore()
     */
    public Store getBackingStore()
    {
        return backingStore;
    }

    /**
     * Convenience method to add a queued operation to the operations we perform at commit.
     * @param oper The operation
     */
    protected void addQueuedOperation(Operation oper)
    {
        ownerSM.getExecutionContext().addOperationToQueue(oper);
    }

    /**
     * Method to update an embedded element in this collection.
     * @param element The element
     * @param fieldNumber Number of field in the element
     * @param value New value for this field
     * @param makeDirty Whether to make the SCO field dirty.
     */
    public void updateEmbeddedElement(E element, int fieldNumber, Object value, boolean makeDirty)
    {
        if (backingStore != null)
        {
            backingStore.updateEmbeddedElement(ownerSM, element, fieldNumber, value);
        }
    }

    /**
     * Method to unset the owner and field information.
     */
    public synchronized void unsetOwner()
    {
        super.unsetOwner();
        if (backingStore != null)
        {
            backingStore = null;
        }
    }

    // ------------------ Implementation of MultiSet methods --------------------

    /**
     * Creates and returns a copy of this object.
     * @return The cloned object
     */
    public Object clone()
    {
        if (useCache)
        {
            loadFromStore();
        }

        // TODO Implement clone()
        return null;
    }

    /**
     * Accessor for whether an element is contained in this Set.
     * @param element The element
     * @return Whether it is contained.
     */
    public boolean contains(Object element)
    {
        if (useCache && isCacheLoaded)
        {
            // If the "delegate" is already loaded, use it
            return delegate.contains(element);
        }
        else if (backingStore != null)
        {
            return backingStore.contains(ownerSM,element);
        }

        return super.contains(element);
    }

    /**
     * Accessor for whether a collection is contained in this Set.
     * @param c The collection
     * @return Whether it is contained.
     */
    public synchronized boolean containsAll(java.util.Collection c)
    {
        if (useCache)
        {
            loadFromStore();
        }
        else if (backingStore != null)
        {
            java.util.HashSet h=new java.util.HashSet(c);
            Iterator iter=iterator();
            while (iter.hasNext())
            {
                h.remove(iter.next());
            }

            return h.isEmpty();
        }

        return super.containsAll(c);
    }

    /* (non-Javadoc)
     * @see com.google.common.collect.ForwardingMultiset#count(java.lang.Object)
     */
    public int count(Object element)
    {
        if (useCache)
        {
            loadFromStore();
        }
        return super.count(element);
    }

    public synchronized boolean equals(Object o)
    {
        if (useCache)
        {
            loadFromStore();
        }
        return super.equals(o);
    }

    public synchronized int hashCode()
    {
        if (useCache)
        {
            loadFromStore();
        }
        return super.hashCode();
    }

    /**
     * Accessor for whether the HashSet is empty.
     * @return Whether it is empty.
     */
    public boolean isEmpty()
    {
        return size() == 0;
    }

    /**
     * Accessor for an iterator for the Set.
     * @return The iterator
     **/
    public Iterator<E> iterator()
    {
        // Populate the cache if necessary
        if (useCache)
        {
            loadFromStore();
        }
        return new SCOCollectionIterator(this, ownerSM, delegate, backingStore, useCache);
    }

    /**
     * Accessor for the size of the HashSet.
     * @return The size.
     **/
    public int size()
    {
        if (useCache && isCacheLoaded)
        {
            // If the "delegate" is already loaded, use it
            return delegate.size();
        }
        else if (backingStore != null)
        {
            return backingStore.size(ownerSM);
        }

        return super.size();
    }

    /**
     * Method to return the list as an array.
     * @return The array
     **/
    public Object[] toArray()
    {
        if (useCache)
        {
            loadFromStore();
        }
        else if (backingStore != null)
        {
            return SCOUtils.toArray(backingStore,ownerSM);
        }  
        return super.toArray();
    }

    /**
     * Method to return the list as an array.
     * @param a The runtime types of the array being defined by this param
     * @return The array
     **/
    public Object[] toArray(Object a[])
    {
        if (useCache)
        {
            loadFromStore();
        }
        else if (backingStore != null)
        {
            return SCOUtils.toArray(backingStore,ownerSM,a);
        }  
        return super.toArray(a);
    }
 
    // ------------------------------ Mutator methods --------------------------

    /**
     * Method to add an element to the HashSet.
     * @param element The new element
     * @return Whether it was added ok.
     **/
    public boolean add(E element)
    {
        // Reject inappropriate elements
        if (!allowNulls && element == null)
        {
            throw new NullPointerException("Nulls not allowed for collection at field " + ownerMmd.getName() + " but element is null");
        }

        if (useCache)
        {
            loadFromStore();
        }
        if (ownerSM != null && ownerSM.getExecutionContext().getManageRelations())
        {
            // Relationship management
            ownerSM.getExecutionContext().getRelationshipManager(ownerSM).relationAdd(ownerMmd.getAbsoluteFieldNumber(), element);
        }

        boolean backingSuccess = true;
        if (backingStore != null)
        {
            if (SCOUtils.useQueuedUpdate(ownerSM))
            {
                addQueuedOperation(new CollectionAddOperation(ownerSM, backingStore, element));
            }
            else
            {
                try
                {
                    backingSuccess = backingStore.add(ownerSM, element, (useCache ? delegate.size() : -1));
                }
                catch (NucleusDataStoreException dse)
                {
                    NucleusLogger.PERSISTENCE.warn(Localiser.msg("023013", "add", ownerMmd.getName(), dse));
                    backingSuccess = false;
                }
            }
        }

        // Only make it dirty after adding the element(s) to the datastore so we give it time
        // to be inserted - otherwise jdoPreStore on this object would have been called before completing the addition
        makeDirty();

        boolean delegateSuccess = delegate.add(element);
        return (backingStore != null ? backingSuccess : delegateSuccess);
    }

    /**
     * Method to add a collection to the HashSet.
     * @param c The collection
     * @return Whether it was added ok.
     **/
    public boolean addAll(Collection c)
    {
        if (useCache)
        {
            loadFromStore();
        }

        if (ownerSM != null && ownerSM.getExecutionContext().getManageRelations())
        {
            // Relationship management
            Iterator iter = c.iterator();
            RelationshipManager relMgr = ownerSM.getExecutionContext().getRelationshipManager(ownerSM);
            while (iter.hasNext())
            {
                relMgr.relationAdd(ownerMmd.getAbsoluteFieldNumber(), iter.next());
            }
        }

        boolean backingSuccess = true;
        if (backingStore != null)
        {
            if (SCOUtils.useQueuedUpdate(ownerSM))
            {
                Iterator iter = c.iterator();
                while (iter.hasNext())
                {
                    addQueuedOperation(new CollectionAddOperation(ownerSM, backingStore, iter.next()));
                }
            }
            else
            {
                try
                {
                    backingSuccess = backingStore.addAll(ownerSM, c, (useCache ? delegate.size() : -1));
                }
                catch (NucleusDataStoreException dse)
                {
                    NucleusLogger.PERSISTENCE.warn(Localiser.msg("023013", "addAll", ownerMmd.getName(), dse));
                    backingSuccess = false;
                }
            }
        }

        // Only make it dirty after adding the element(s) to the datastore so we give it time
        // to be inserted - otherwise jdoPreStore on this object would have been called before completing the addition
        makeDirty();

        boolean delegateSuccess = delegate.addAll(c);
        return (backingStore != null ? backingSuccess : delegateSuccess);
    }

    /**
     * Method to clear the HashSet
     **/
    public void clear()
    {
        makeDirty();

        if (backingStore != null)
        {
            if (SCOUtils.useQueuedUpdate(ownerSM))
            {
                addQueuedOperation(new CollectionClearOperation(ownerSM, backingStore));
            }
            else
            {
                backingStore.clear(ownerSM);
            }
        }
        delegate.clear();
    }

    /**
     * Method to remove an element from the HashSet.
     * @param element The element
     * @return Whether it was removed ok.
     **/
    public synchronized boolean remove(Object element)
    {
        return remove(element, true);
    }

    /**
     * Method to remove an element from the collection, and observe the flag for whether to allow cascade delete.
     * @param element The element
     * @param allowCascadeDelete Whether to allow cascade delete
     */
    public synchronized boolean remove(Object element, boolean allowCascadeDelete)
    {
        makeDirty();

        if (useCache)
        {
            loadFromStore();
        }

        int size = (useCache ? delegate.size() : -1);
        boolean contained = delegate.contains(element);
        boolean delegateSuccess = delegate.remove(element);
        if (ownerSM != null && ownerSM.getExecutionContext().getManageRelations())
        {
            ownerSM.getExecutionContext().getRelationshipManager(ownerSM).relationRemove(ownerMmd.getAbsoluteFieldNumber(), element);
        }

        boolean backingSuccess = true;
        if (backingStore != null)
        {
            if (SCOUtils.useQueuedUpdate(ownerSM))
            {
                backingSuccess = contained;
                if (backingSuccess)
                {
                    addQueuedOperation(new CollectionRemoveOperation(ownerSM, backingStore, element, allowCascadeDelete));
                }
            }
            else
            {
                try
                {
                    backingSuccess = backingStore.remove(ownerSM, element, size, allowCascadeDelete);
                }
                catch (NucleusDataStoreException dse)
                {
                    NucleusLogger.PERSISTENCE.warn(Localiser.msg("023013", "remove", ownerMmd.getName(), dse));
                    backingSuccess = false;
                }
            }
        }

        return (backingStore != null ? backingSuccess : delegateSuccess);
    }

    /**
     * Method to remove all elements from the collection from the HashSet.
     * @param c The collection of elements to remove 
     * @return Whether it was removed ok.
     **/
    public boolean removeAll(java.util.Collection c)
    {
        makeDirty();
 
        if (useCache)
        {
            loadFromStore();
        }

        if (ownerSM != null && ownerSM.getExecutionContext().getManageRelations())
        {
            // Relationship management
            Iterator iter = c.iterator();
            RelationshipManager relMgr = ownerSM.getExecutionContext().getRelationshipManager(ownerSM);
            while (iter.hasNext())
            {
                relMgr.relationRemove(ownerMmd.getAbsoluteFieldNumber(), iter.next());
            }
        }

        if (backingStore != null)
        {
            boolean backingSuccess = true;
            int size = (useCache ? delegate.size() : -1);

            if (SCOUtils.useQueuedUpdate(ownerSM))
            {
                backingSuccess = false;
                Iterator iter = c.iterator();
                while (iter.hasNext())
                {
                    Object element = iter.next();
                    if (contains(element))
                    {
                        backingSuccess = true;
                        addQueuedOperation(new CollectionRemoveOperation(ownerSM, backingStore, element, true));
                    }
                }
            }
            else
            {
                try
                {
                    backingSuccess = backingStore.removeAll(ownerSM, c, size);
                }
                catch (NucleusDataStoreException dse)
                {
                    NucleusLogger.PERSISTENCE.warn(Localiser.msg("023013", "removeAll", ownerMmd.getName(), dse));
                    backingSuccess = false;
                }
            }

            delegate.removeAll(c); // Remove from the delegate too
            return backingSuccess;
        }

        return delegate.removeAll(c);
    }

    /**
     * Method to retain a Collection of elements (and remove all others).
     * @param c The collection to retain
     * @return Whether they were retained successfully.
     **/
    public synchronized boolean retainAll(java.util.Collection c)
    {
        makeDirty();

        if (useCache)
        {
            loadFromStore();
        }

        boolean modified = false;
        Iterator iter = iterator();
        while (iter.hasNext())
        {
            Object element = iter.next();
            if (!c.contains(element))
            {
                iter.remove();
                modified = true;
            }
        }
        return modified;
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
     * 
     * @return the replaced object
     * @throws ObjectStreamException if an error occurs
     */
    protected Object writeReplace() throws ObjectStreamException
    {
        if (useCache)
        {
            loadFromStore();
            HashMultiset multi = HashMultiset.create();
            multi.addAll(delegate);
            return multi;
        }

        // TODO Cater for non-cached collection, load elements in a DB call.
        HashMultiset multi = HashMultiset.create();
        multi.addAll(delegate);
        return multi;
    }
}