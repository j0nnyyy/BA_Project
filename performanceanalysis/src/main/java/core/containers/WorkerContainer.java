package core.containers;

import core.AppInformation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public class WorkerContainer implements Iterable<CoreContainer> {

    private ArrayList<CoreContainer> coreContainers = new ArrayList<>();
    private int workerCount;

    public WorkerContainer(int workerCount) {
        this.workerCount = workerCount;
    }

    public boolean add(AppInformation info) {

        if(workerCount == info.getWorkers()) {
            for(CoreContainer container : coreContainers) {
                if(container.add(info)) {
                    return true;
                }
            }

            CoreContainer newContainer = new CoreContainer(info.getCores());
            newContainer.add(info);
            coreContainers.add(newContainer);
            return true;
        }

        return false;

    }

    public void sort() {

        CoreContainer[] containers = coreContainers.toArray(new CoreContainer[coreContainers.size()]);

        for(int i = 1; i < containers.length; i++) {
            for(int j = i; j > 0 && containers[j].getCoreCount() > containers[j - 1].getCoreCount(); j--) {
                CoreContainer tmp = containers[j - 1];
                containers[j - 1] = containers[j];
                containers[j] = tmp;
            }
        }

        coreContainers = new ArrayList<>(Arrays.asList(containers));

    }

    public AppInformation getInformation(int cores) {

        for(CoreContainer container : coreContainers) {
            if(container.getCoreCount() == cores) {
                return container.getInformation();
            }
        }

        return null;

    }

    public ArrayList<Integer> getCoreCounts() {

        ArrayList<Integer> tmp = new ArrayList<>();

        for(CoreContainer container : coreContainers) {
            tmp.add(container.getCoreCount());
        }

        return tmp;

    }

    public void print() {

        System.out.println("        " + workerCount);

        for(CoreContainer container : coreContainers) {
            container.print();
        }

    }

    public int getWorkerCount() {
        return workerCount;
    }

    @Override
    public Iterator<CoreContainer> iterator() {
        return coreContainers.iterator();
    }
}
