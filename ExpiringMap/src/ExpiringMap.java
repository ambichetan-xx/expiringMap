import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * An expiring map is a Map in which entries are evicted of the map after their
 * time to live expired.
 * 
 * If an map entry hasn't been accessed for <code>
 * timeToLiveMillis</code> the map entry is evicted out of the map, subsequent
 * to which an attempt to get the key from the map will return null.
 * 
 * @param <K>
 * @param <V>
 */
public class ExpiringMap<K, V> implements Map<K, V> {

	// ConcurrentHashMap is used as the internal Map to hold the map entries
	private ConcurrentHashMap<K, ExpiringObject> internalMap = null;

	private Expirer expirer = null;

	// method used to set time for the map values
	public void ExpiringMap(long timeToLiveInMillis) {
		this.setExpiringMapValues(new ConcurrentHashMap<K, ExpiringObject>(),
				timeToLiveInMillis, 1000);
	}

	// set the time required values for expiring map members
	private void setExpiringMapValues(
			ConcurrentHashMap<K, ExpiringObject> internalMap, long timeToLive,
			long expirationInterval) {
		this.internalMap = internalMap;
		this.expirer = new Expirer();
		expirer.setTimeToLive(timeToLive);
		// set expirationInterval to 1 sec, this means expiring thread checks
		// for expired values every second
		expirer.setExpirationInterval(expirationInterval);
	}

	@Override
	public int size() {
		return internalMap.size();
	}

	@Override
	public boolean isEmpty() {
		return internalMap.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		return internalMap.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return internalMap.containsValue(value);
	}

	@Override
	// reset the last access time of an entry, when it is accessed through
	// get(key) function
	public V get(Object key) {
		ExpiringObject object = internalMap.get(key);

		if (object != null) {
			object.setLastAccessTime(System.currentTimeMillis());
			return object.getValue();
		}

		return null;
	}

	@Override
	// set the current time as the last access time while putting new entry to
	// the map
	public V put(K key, V value) {
		ExpiringObject entry = internalMap.put(key, new ExpiringObject(key,
				value, System.currentTimeMillis()));
		if (entry == null) {
			return null;
		}

		return entry.getValue();
	}

	@Override
	public V remove(Object key) {
		ExpiringObject answer = internalMap.remove(key);
		if (answer == null) {
			return null;
		}
		return answer.getValue();
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		for (Entry<? extends K, ? extends V> e : m.entrySet()) {
			this.put(e.getKey(), e.getValue());
		}
	}

	@Override
	public void clear() {
		internalMap.clear();
	}

	@Override
	public Set<K> keySet() {
		return internalMap.keySet();
	}

	@Override
	public Collection<V> values() {
		// as of now I have not supported this functionality, but it can be
		// implemented similar to entrySet()
		throw new UnsupportedOperationException();
	}

	@Override
	// populate Set<java.util.Map.Entry<K, V>> and return
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		Set<java.util.Map.Entry<K, V>> set = new HashSet<Map.Entry<K, V>>();
		Iterator iterator = internalMap.entrySet().iterator();
		while (iterator.hasNext()) {
			Entry<K, ExpiringObject> internalEntry = (Map.Entry<K, ExpiringObject>) iterator
					.next();
			Map.Entry<K, V> entry = new AbstractMap.SimpleEntry<K, V>(
					(K) internalEntry.getValue().key,
					(V) internalEntry.getValue().value);
			set.add((java.util.Map.Entry<K, V>) entry);
		}
		return set;
	}

	public Expirer getExpirer() {
		return expirer;
	}

	public void setExpirer(Expirer expirer) {
		this.expirer = expirer;
	}

	// Customized object to hold the map entries which has the additional member
	// variable lastAccessTime
	private class ExpiringObject {
		private K key;

		private V value;

		// additional member variable to hold the last accessed value
		private long lastAccessTime;

		// to avoid race
		private final ReadWriteLock lastAccessTimeLock = new ReentrantReadWriteLock();

		ExpiringObject(K key, V value, long lastAccessTime) {
			if (value == null) {
				throw new IllegalArgumentException(
						"An expiring object cannot be null.");
			}
			this.key = key;
			this.value = value;
			this.lastAccessTime = lastAccessTime;
		}

		public long getLastAccessTime() {
			lastAccessTimeLock.readLock().lock();

			try {
				return lastAccessTime;
			} finally {
				lastAccessTimeLock.readLock().unlock();
			}
		}

		public void setLastAccessTime(long lastAccessTime) {
			lastAccessTimeLock.writeLock().lock();

			try {
				this.lastAccessTime = lastAccessTime;
			} finally {
				lastAccessTimeLock.writeLock().unlock();
			}
		}

		public K getKey() {
			return key;
		}

		public V getValue() {
			return value;
		}

		@Override
		public boolean equals(Object obj) {
			return value.equals(obj);
		}

		@Override
		public int hashCode() {
			return value.hashCode();
		}
	}

	public class Expirer implements Runnable {
		private final ReadWriteLock stateLock = new ReentrantReadWriteLock();

		private long expirationIntervalMillis;

		private long timeToLiveMillis;

		private boolean running = false;

		private final Thread expirerThread;

		public Expirer() {
			expirerThread = new Thread(this);
			expirerThread.setDaemon(true);
		}

		public void run() {
			while (running) {
				removeExpiredEntries();

				try {
					Thread.sleep(expirationIntervalMillis);
				} catch (InterruptedException e) {
				}
			}
		}

		// this functionality removes the expired values from the internal map
		private void removeExpiredEntries() {
			long timeNow = System.currentTimeMillis();

			for (ExpiringObject o : internalMap.values()) {

				if (timeToLiveMillis <= 0) {
					continue;
				}

				long timeIdle = timeNow - o.getLastAccessTime();

				// if the idle time is more that the time to live milliseconds,
				// remove it
				if (timeIdle >= timeToLiveMillis) {
					internalMap.remove(o.getKey());
				}
			}
		}

		/**
		 * Kick off this thread which will look for old objects and remove them.
		 * 
		 */
		public void startExpiring() {
			stateLock.writeLock().lock();

			try {
				if (!running) {
					running = true;
					expirerThread.start();
				}
			} finally {
				stateLock.writeLock().unlock();
			}
		}

		/**
		 * Stop the thread from monitoring the map.
		 */
		public void stopExpiring() {
			stateLock.writeLock().lock();

			try {
				if (running) {
					running = false;
					expirerThread.interrupt();
				}
			} finally {
				stateLock.writeLock().unlock();
			}
		}

		/**
		 * Checks to see if the thread is running
		 */
		public boolean isRunning() {
			stateLock.readLock().lock();

			try {
				return running;
			} finally {
				stateLock.readLock().unlock();
			}
		}

		/**
		 * Returns the Time-to-live value.
		 */
		public int getTimeToLive() {
			stateLock.readLock().lock();

			try {
				return (int) timeToLiveMillis / 1000;
			} finally {
				stateLock.readLock().unlock();
			}
		}

		/**
		 * Update the value for the time-to-live
		 */
		public void setTimeToLive(long timeToLive) {
			stateLock.writeLock().lock();

			try {
				this.timeToLiveMillis = timeToLive * 1000;
			} finally {
				stateLock.writeLock().unlock();
			}
		}

		/**
		 * Get the interval in which an object will live in the map before it is
		 * removed.
		 */
		public int getExpirationInterval() {
			stateLock.readLock().lock();

			try {
				return (int) expirationIntervalMillis / 1000;
			} finally {
				stateLock.readLock().unlock();
			}
		}

		/**
		 * Set the interval in which an object will live in the map before it is
		 * removed. The time in seconds
		 */
		public void setExpirationInterval(long expirationInterval) {
			stateLock.writeLock().lock();

			try {
				this.expirationIntervalMillis = expirationInterval * 1000;
			} finally {
				stateLock.writeLock().unlock();
			}
		}
	}

	public static void main(String[] args) {

		ExpiringMap<Integer, Integer> expiringMap = new ExpiringMap<Integer, Integer>();
		// set timeToLiveInMillis to 2 secs
		expiringMap.ExpiringMap(2000);
		// start expiring thread
		expiringMap.getExpirer().startExpiring();
		expiringMap.put(10, 10);
		expiringMap.put(20, 20);

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		// accessing 10 here, so that it's last access time is reset
		expiringMap.get(10);

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		expiringMap.put(30, 30);

		// only 10 and 30 should be alive, since 20 is not accessed from 2 secs,
		// it will be automatically removed from the map
		Iterator<Entry<Integer, Integer>> iterator = expiringMap.entrySet()
				.iterator();
		while (iterator.hasNext()) {
			System.out.println(iterator.next());
		}

	}
}
