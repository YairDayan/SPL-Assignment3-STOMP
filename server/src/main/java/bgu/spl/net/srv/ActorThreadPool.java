package bgu.spl.net.srv;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A thread pool that runs tasks on behalf of actors.
 * Tasks of the same actor run one at a time, in the order they were submitted,
 * while tasks of different actors may run in parallel. The reactor uses each
 * connection handler as an actor, so the messages of a single client are
 * processed in order and never concurrently.
 */
public class ActorThreadPool {

    private final Map<Object, Queue<Runnable>> acts;
    private final ReadWriteLock actsRWLock;
    private final Set<Object> playingNow;
    private final ExecutorService threads;

    /**
     * Creates a pool with a fixed number of threads.
     *
     * @param threads the number of threads in the pool
     */
    public ActorThreadPool(int threads) {
        this.threads = Executors.newFixedThreadPool(threads);
        acts = new WeakHashMap<>();
        playingNow = ConcurrentHashMap.newKeySet();
        actsRWLock = new ReentrantReadWriteLock();
    }

    /**
     * Submits a task on behalf of an actor. The task runs immediately if the
     * actor has no running task; otherwise it is queued until the actor's
     * earlier tasks complete.
     *
     * @param act the actor the task belongs to
     * @param r   the task to run
     */
    public void submit(Object act, Runnable r) {
        synchronized (act) {
            if (!playingNow.contains(act)) {
                playingNow.add(act);
                execute(r, act);
            } else {
                pendingRunnablesOf(act).add(r);
            }
        }
    }

    /**
     * Stops the pool, interrupting the running tasks.
     */
    public void shutdown() {
        threads.shutdownNow();
    }

    /**
     * Returns the queue of tasks waiting for an actor, creating it if it does
     * not exist yet.
     *
     * @param act the actor
     * @return the actor's queue of pending tasks
     */
    private Queue<Runnable> pendingRunnablesOf(Object act) {

        actsRWLock.readLock().lock();
        Queue<Runnable> pendingRunnables = acts.get(act);
        actsRWLock.readLock().unlock();

        if (pendingRunnables == null) {
            actsRWLock.writeLock().lock();
            acts.put(act, pendingRunnables = new LinkedList<>());
            actsRWLock.writeLock().unlock();
        }
        return pendingRunnables;
    }

    /**
     * Runs an actor's task on one of the pool's threads, and handles the
     * actor's next task when it completes.
     *
     * @param r   the task to run
     * @param act the actor the task belongs to
     */
    private void execute(Runnable r, Object act) {
        threads.execute(() -> {
            try {
                r.run();
            } finally {
                complete(act);
            }
        });
    }

    /**
     * Called when an actor's task completes. Runs the actor's next pending
     * task, or marks the actor as idle if it has none.
     *
     * @param act the actor whose task completed
     */
    private void complete(Object act) {
        synchronized (act) {
            Queue<Runnable> pending = pendingRunnablesOf(act);
            if (pending.isEmpty()) {
                playingNow.remove(act);
            } else {
                execute(pending.poll(), act);
            }
        }
    }

}
