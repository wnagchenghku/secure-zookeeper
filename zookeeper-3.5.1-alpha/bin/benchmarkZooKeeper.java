/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class benchmarkZooKeeper {
	private int numOfThreads;
	private String hostPort;
	private int numberOps;
	private boolean sync = true;
	private ArrayList<Thread> clients;
	    
	public benchmarkZooKeeper(int threads, String hp, int ops) {
		numOfThreads = threads;
		hostPort = hp;
		numberOps = ops;
		clients = new ArrayList<Thread>();
	}
    
    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: java -cp $CLASSPATH benchmarkZooKeeper #threads hostPort #ops");
            System.exit(1);
        }
        new benchmarkZooKeeper(Integer.parseInt(args[0]), args[1], Integer.parseInt(args[2])).go();
    }
    public void go() {
    	try {
	    	for (int i = 0; i < numOfThreads; i++) {
					Thread t = new Thread(new ClientHandler());
					t.start();
					clients.add(t);
	    	}
			for (Thread c: clients) {
				c.join();
			}
	    } catch(IOException | InterruptedException e) {
	    }
    }

    private class ClientHandler implements Runnable, Watcher {
	    private volatile boolean connected = false;
	    private volatile boolean expired = false;

    	public ClientHandler() throws IOException {
		}

	    public void process(WatchedEvent e) {  
	        System.out.println("Processing event: " + e.toString());
	        if(e.getType() == Event.EventType.None){
	            switch (e.getState()) {
	            case SyncConnected:
	                connected = true;
	                break;
	            case Disconnected:
	                connected = false;
	                break;
	            case Expired:
	                expired = true;
	                connected = false;
	            default:
	                break;
	            }
	        }
	    }
	    public boolean isConnected() {
	    	return connected;
	    }

		public void run() {
			try {
				String path = "/master" + String.valueOf(Thread.currentThread().getId());
				ZooKeeper zk = new ZooKeeper(hostPort, 15000, this);
				while(!isConnected()) {
					Thread.sleep(100);
				}
				byte[] payload = new byte[1024];
				zk.create(path, payload, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				long start = System.currentTimeMillis();
				if (sync) {
					for (int i = 0; i < numberOps; i++) {
						if (Math.random() <= 0.7) {
						    zk.getData(path, false, null);
						} else {
							zk.setData(path, payload, -1);
						}
					}
				} else {
					LinkedList<Integer> results = new LinkedList<Integer>();
				    DataCallback checkCallback = new DataCallback() {
				        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
				            switch (Code.get(rc)) {
				            case OK:
				                synchronized(ctx) {
				                    ((LinkedList<Integer>)ctx).add(rc);
				                    ctx.notifyAll();
				                }
				                break;
				            default:
				                System.out.println("Error when reading data." + KeeperException.create(Code.get(rc), path)); 
				            }
				        }
				    };
		            for (int i = 0; i < numberOps; i++) {
		            	zk.getData(path, false, checkCallback, results);
		            }		
		            synchronized (results) {
		                while (results.size() < numberOps) {
		                    results.wait();
		                }
		            }
				}
				long end = System.currentTimeMillis();
				System.out.println("thread " + Thread.currentThread().getId() + ", " + numberOps + " ops, " + (end - start) + " milliseconds");
				zk.delete(path, -1);
				zk.close();
			} catch (IOException | InterruptedException | KeeperException e) {
			} finally {
			}
		}
	}
}