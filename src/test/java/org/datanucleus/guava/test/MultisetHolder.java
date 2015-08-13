package org.datanucleus.guava.test;

import javax.jdo.annotations.PersistenceCapable;

import com.google.common.collect.Multiset;

@PersistenceCapable
public class MultisetHolder {

	private Multiset<String> words;

	public MultisetHolder(Multiset<String> words) {
		this.words = words;
	}

	public Multiset<String> getWords() {
		return words;
	}
}
