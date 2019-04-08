package core.containers;

import com.sun.corba.se.spi.orbutil.threadpool.Work;
import core.AppInformation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public class DataSizeContainer implements Iterable<WorkerContainer> {

    private ArrayList<WorkerContainer> workerContainers = new ArrayList<>();
    private long dataSize;

    public DataSizeContainer(long dataSize) {
        this.dataSize = dataSize;
    }

    public boolean add(AppInformation info) {

        if(info.getDataSize() == dataSize) {
            for(WorkerContainer container : workerContainers) {
                if(container.add(info)) {
                    return true;
                }
            }

            WorkerContainer newContainer = new WorkerContainer(info.getWorkers());
            newContainer.add(info);
            workerContainers.add(newContainer);
            return true;
        }

        return false;

    }

    public void sort() {

        for(WorkerContainer container : workerContainers) {
            container.sort();
        }

        WorkerContainer[] containers = workerContainers.toArray(new WorkerContainer[workerContainers.size()]);

        for(int i = 1; i < containers.length; i++) {
            for(int j = i; j > 0 && containers[j].getWorkerCount() < containers[j - 1].getWorkerCount(); j--) {
                WorkerContainer tmp = containers[j - 1];
                containers[j - 1] = containers[j];
                containers[j] = tmp;
            }
        }

        workerContainers = new ArrayList<>(Arrays.asList(containers));

    }

    public AppInformation getInformation(int workers, int cores) {

        for(WorkerContainer container : workerContainers) {
            if(container.getWorkerCount() == workers) {
                return container.getInformation(cores);
            }
        }

        return null;

    }

    public ArrayList<Integer> getWorkerCounts() {

        ArrayList<Integer> tmp = new ArrayList<>();

        for(WorkerContainer container : workerContainers) {
            tmp.add(container.getWorkerCount());
        }

        return tmp;

    }

    public ArrayList<Integer> getCoreCounts(int workerCount) {

        for(WorkerContainer container : workerContainers) {
            if(container.getWorkerCount() == workerCount)
                return container.getCoreCounts();
        }

        return null;

    }

    public long getDataSize() {
        return dataSize;
    }

    public void print() {

        System.out.println("    " + dataSize);

        for(WorkerContainer container : workerContainers) {
            container.print();
        }

    }

    @Override
    public Iterator<WorkerContainer> iterator() {
        return workerContainers.iterator();
    }

}
