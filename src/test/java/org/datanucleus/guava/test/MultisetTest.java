package org.datanucleus.guava.test;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Transaction;

import org.datanucleus.util.NucleusLogger;
import org.junit.Test;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

public class MultisetTest
{
    @Test
    public void testPersist()
    {
        PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory("GuavaTest");

        PersistenceManager pm = pmf.getPersistenceManager();
        Transaction tx = pm.currentTransaction();
        try
        {
            tx.begin();

            HashMultiset<String> words = HashMultiset.create();
            words.add("banana");
            words.add("car");
            words.add("moon");
            words.add("car");
            
            MultisetHolder multisetHolder = new MultisetHolder(words);
            
            pm.makePersistent(multisetHolder);
            Object id = JDOHelper.getObjectId(multisetHolder);
            tx.commit();
            pm.close();

            pm = pmf.getPersistenceManager();
            tx = pm.currentTransaction();
            tx.begin();
            
            MultisetHolder loadedHolder = (MultisetHolder) pm.getObjectById(id);
            Multiset<String> loadedWords = loadedHolder.getWords();
            
            assertEquals(loadedWords.count("banana"), 1);
            assertEquals(loadedWords.count("car"), 2);
            assertEquals(loadedWords.count("moon"), 1);
            assertEquals(loadedWords.count("dog"), 0);
            
            tx.commit();
        }
        catch (Throwable thr)
        {
            NucleusLogger.GENERAL.error(">> Exception in test", thr);
            fail("Failed test : " + thr.getMessage());
        }
        finally 
        {
            if (tx.isActive())
            {
                tx.rollback();
            }
            pm.close();
        }

        pmf.close();
    }
}
