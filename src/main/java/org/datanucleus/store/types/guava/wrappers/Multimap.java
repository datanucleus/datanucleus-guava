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
import java.util.Map;

import org.datanucleus.api.ApiAdapter;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.state.FetchPlanState;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.types.SCOMap;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

import com.google.common.collect.ForwardingMultimap;
import com.google.common.collect.HashMultimap;

/**
 * A mutable second-class Multimap object.
 * This is the simplified form that intercepts mutators and marks the field as dirty.
 */
public class Multimap<K, V> extends ForwardingMultimap<K, V> implements SCOMap, Cloneable, java.io.Serializable
{
    protected transient ObjectProvider ownerOP;
    protected transient AbstractMemberMetaData ownerMmd;

    /** The internal "delegate". */
    protected com.google.common.collect.Multimap<K, V> delegate;

    /**
     * Constructor, using the ObjectProvider of the "owner" and the field name.
     * @param op The owner ObjectProvider
     * @param mmd Metadata for the member
     */
    public Multimap(ObjectProvider op, AbstractMemberMetaData mmd)
    {
        this.ownerOP = op;
        this.ownerMmd = mmd;
    }

    /**
     * Method to initialise the SCO from an existing value.
     * @param o  The object to set from
     * @param forInsert Whether the object needs inserting in the datastore with this value
     * @param forUpdate Whether to update the datastore with this value
     */
    public synchronized void initialise(Object o, boolean forInsert, boolean forUpdate)
    {
        com.google.common.collect.Multimap<K, V> m = (com.google.common.collect.Multimap<K, V>) o;
        delegate = HashMultimap.create();
        if (m != null)
        {
            delegate.putAll(m);
        }
        if (NucleusLogger.PERSISTENCE.isDebugEnabled())
        {
            NucleusLogger.PERSISTENCE.debug(Localiser.msg("023003", 
                ownerOP.getObjectAsPrintable(), ownerMmd.getName(), "" + size(), 
                SCOUtils.getSCOWrapperOptionsMessage(true, false, true, false)));
        }
    }

    /**
     * Method to initialise the SCO for use.
     */
    public void initialise()
    {
        delegate = HashMultimap.create();
        if (NucleusLogger.PERSISTENCE.isDebugEnabled())
        {
            NucleusLogger.PERSISTENCE.debug(Localiser.msg("023003", 
                ownerOP.getObjectAsPrintable(), ownerMmd.getName(), "" + size(), 
                SCOUtils.getSCOWrapperOptionsMessage(true, false, true, false)));
        }
    }

    // ------------------------ Implementation of SCO methods ------------------

    /**
     * Accessor for the unwrapped value that we are wrapping.
     * @return The unwrapped value
     */
    public Object getValue()
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
     * Method to update an embedded key in this map.
     * @param key The key
     * @param fieldNumber Number of field in the key
     * @param newValue New value for this field
     */
    public void updateEmbeddedKey(Object key, int fieldNumber, Object newValue)
    {
        // Just mark field in embedded owners as dirty
        makeDirty();
    }

    /**
     * Method to update an embedded value in this map.
     * @param value The value
     * @param fieldNumber Number of field in the value
     * @param newValue New value for this field
     */
    public void updateEmbeddedValue(Object value, int fieldNumber, Object newValue)
    {
        // Just mark field in embedded owners as dirty
        makeDirty();
    }

    /**
     * Accessor for the field name.
     * @return The field name.
     */
    public String getFieldName()
    {
        return ownerMmd.getName();
    }

    /**
     * Accessor for the owner.
     * @return The owner.
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
     */
    public void makeDirty()
    {
        if (ownerOP != null)
        {
            ownerOP.makeDirty(ownerMmd.getAbsoluteFieldNumber());
        }
    }

    /**
     * Method to return a detached copy of the container.
     * Recurses through the keys/values so that they are likewise detached.
     * @param state State for detachment process
     * @return The detached container
     */
    public Object detachCopy(FetchPlanState state)
    {
        com.google.common.collect.Multimap<K, V> detached = HashMultimap.create();
        ApiAdapter api = ownerOP.getExecutionContext().getApiAdapter();
        Collection<Map.Entry<K, V>> entries = entries();
        for (Iterator<Map.Entry<K, V>> it = entries.iterator(); it.hasNext();)
        {
            Map.Entry<K, V> entry = it.next();
            K key = entry.getKey();
            V val = entry.getValue();
            if (api.isPersistable(key))
            {
                key = ownerOP.getExecutionContext().detachObjectCopy(key, state);
            }
            if (api.isPersistable(val))
            {
                val = ownerOP.getExecutionContext().detachObjectCopy(val, state);
            }
            detached.put(key, val);
        }
        return detached;
    }

    /**
     * Method to return an attached copy of the passed (detached) value. The returned attached copy
     * is a SCO wrapper. Goes through the existing keys/values in the store for this owner field and
     * removes ones no longer present, and adds new keys/values. All keys/values in the (detached)
     * value are attached.
     * @param value The new (map) value
     */
    public void attachCopy(Object value)
    {
        com.google.common.collect.Multimap<K, V> m = (com.google.common.collect.Multimap<K, V>) value;

        // Attach all of the keys/values in the new map
        boolean keysWithoutIdentity = SCOUtils.mapHasKeysWithoutIdentity(ownerMmd);
        boolean valuesWithoutIdentity = SCOUtils.mapHasValuesWithoutIdentity(ownerMmd);

        com.google.common.collect.Multimap<K, V> attachedKeysValues = HashMultimap.create();
        Collection<Map.Entry<K, V>> detachedEntries = m.entries();
        Iterator<Map.Entry<K, V>> iter = detachedEntries.iterator();
        ApiAdapter api = ownerOP.getExecutionContext().getApiAdapter();
        while (iter.hasNext())
        {
            Map.Entry<K, V> entry = iter.next();
            K key = entry.getKey();
            V val = entry.getValue();
            if (api.isPersistable(key) && api.isDetachable(key))
            {
                key = ownerOP.getExecutionContext().attachObjectCopy(ownerOP, key, keysWithoutIdentity);
            }
            if (api.isPersistable(val) && api.isDetachable(val))
            {
                val = ownerOP.getExecutionContext().attachObjectCopy(ownerOP, val, valuesWithoutIdentity);
            }
            attachedKeysValues.put(key, val);
        }

        // Update the attached map with the detached entries
        com.google.common.collect.Multimap<K, V> copy = HashMultimap.create();
        copy.putAll(this);

        // Delete any keys that are no longer in the Map
        Iterator<Map.Entry<K, V>> attachedIter = copy.entries().iterator();
        while (attachedIter.hasNext())
        {
            Map.Entry<K, V> entry = attachedIter.next();
            K key = entry.getKey();
            if (!attachedKeysValues.containsKey(key))
            {
                this.removeAll(key);
            }
        }

        // Add any new keys/values and update any changed values
        Iterator<Map.Entry<K, V>> keysIter = attachedKeysValues.entries().iterator();
        while (keysIter.hasNext())
        {
            Map.Entry<K, V> entry = keysIter.next();
            K theKey = entry.getKey();
            V theValue = entry.getValue();
            if (!this.containsKey(theKey))
            {
                // Not present so add it
                this.put(theKey, theValue);
            }
            else
            {
                // Update any values
                Object oldValue = this.get(theKey);
                if (api.isPersistable(theValue) && api.getIdForObject(theValue) != api.getIdForObject(oldValue))
                {
                    // In case they have changed the PC for this key (different id)
                    this.put(theKey, theValue);
                }
                else
                {
                    if ((oldValue == null && theValue != null) || (oldValue != null && !oldValue.equals(theValue)))
                    {
                        this.put(theKey, theValue);
                    }
                }
            }
        }
    }

    // -------------------- Implementation of Map Methods ----------------------
 
    /**
     * Creates and returns a copy of this object.
     * <P>Mutable second-class Objects are required to provide a public clone method in order to allow for copying persistable objects. 
     * In contrast to Object.clone(), this method must not throw a CloneNotSupportedException.
     * @return Clone of the object
     */
    public Object clone()
    {
        return ((java.util.HashMap)delegate).clone();
    }

    /**
     * Method to clear the Map.
     */
    public synchronized void clear()
    {
        delegate.clear();
        makeDirty();
    }

    /**
     * Method to add a value to the Map.
     * @param key The key for the value.
     * @param value The value
     * @return Whether it was successful
     */
    public synchronized boolean put(K key, V value)
    {
        // Reject inappropriate elements
        boolean success = delegate.put(key, value);
        makeDirty();
        return success;
    }

    /**
     * Method to add a Map of values to this map.
     * @param multi The Map to add
     * @return Whether it was successful
     */
    public synchronized boolean putAll(Multimap<K, V> multi)
    {
        boolean success = delegate.putAll(multi);
        makeDirty();
        return success;
    }

    /**
     * Method to add a key with several values to the map.
     * @param key The key
     * @param values The values for this key
     * @return Whether it was successful
     */
    public synchronized boolean putAll(K key, Iterable<? extends V> values)
    {
        boolean success = delegate.putAll(key, values);
        makeDirty();
        return success;
    }

    /**
     * Method to remove a (key,value) from the Map.
     * @param key The key
     * @param value The value for this key
     * @return Whether it was successful
     */
    public synchronized boolean remove(Object key, Object value)
    {
        boolean success = delegate.remove(key, value);
        makeDirty();
        return success;
    }

    /**
     * Method to remove all values for the specified key from the Map.
     * @param key The key
     * @return The values for this key that were removed
     */
    public synchronized Collection<V> removeAll(Object key)
    {
        Collection<V> vals = delegate.removeAll(key);
        makeDirty();
        return vals;
    }

    /**
     * The writeReplace method is called when ObjectOutputStream is preparing
     * to write the object to the stream. The ObjectOutputStream checks whether
     * the class defines the writeReplace method. If the method is defined, the
     * writeReplace method is called to allow the object to designate its
     * replacement in the stream. The object returned should be either of the
     * same type as the object passed in or an object that when read and
     * resolved will result in an object of a type that is compatible with all
     * references to the object.
     * @return the replaced object
     * @throws ObjectStreamException if an error occurs
     */
    protected Object writeReplace() throws ObjectStreamException
    {
        HashMultimap<K, V> multi = HashMultimap.create();
        multi.putAll(delegate);
        return multi;
    }

    @Override
    protected com.google.common.collect.Multimap<K, V> delegate()
    {
        return delegate;
    }
}