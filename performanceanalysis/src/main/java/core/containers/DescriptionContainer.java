package core.containers;

import core.AppInformation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public class DescriptionContainer implements Iterable<DataSizeContainer> {

    private ArrayList<DataSizeContainer> dataSizeContainers = new ArrayList<>();
    private String description;

    public DescriptionContainer(String description) {
        this.description = description;
    }

    public boolean add(AppInformation info) {

        if(info.getDescription().equals(description)) {
            for(DataSizeContainer container : dataSizeContainers) {
                if(container.add(info)) {
                    return true;
                }
            }

            DataSizeContainer newContainer = new DataSizeContainer(info.getDataSize());
            newContainer.add(info);
            dataSizeContainers.add(newContainer);
            return true;
        }

        return false;

    }

    public void sort() {

        for(DataSizeContainer container : dataSizeContainers) {
            container.sort();
        }

        DataSizeContainer[] containers = dataSizeContainers.toArray(new DataSizeContainer[dataSizeContainers.size()]);

        for(int i = 1; i < containers.length; i++) {
            for(int j = i; j > 0 && containers[j].getDataSize() < containers[j - 1].getDataSize(); j--) {
                DataSizeContainer tmp = containers[j - 1];
                containers[j - 1] = containers[j];
                containers[j] = tmp;
            }
        }

        dataSizeContainers = new ArrayList<>(Arrays.asList(containers));

    }

    public AppInformation getInformation(long dataSize, int workers, int cores) {

        for(DataSizeContainer container : dataSizeContainers) {
            if(container.getDataSize() == dataSize) {
                return container.getInformation(workers, cores);
            }
        }

        return null;

    }

    public ArrayList<Long> getDataSizes() {

        ArrayList<Long> tmp = new ArrayList<>();

        for(DataSizeContainer container : dataSizeContainers) {
            tmp.add(container.getDataSize());
        }

        return tmp;

    }

    public ArrayList<Integer> getWorkerCounts(long datasize) {

        for(DataSizeContainer container : dataSizeContainers) {
            if(container.getDataSize() == datasize)
                return container.getWorkerCounts();
        }

        return null;

    }

    public ArrayList<Integer> getCoreCounts(long datasize, int workerCount) {

        for(DataSizeContainer container : dataSizeContainers) {
            if(container.getDataSize() == datasize)
                return container.getCoreCounts(workerCount);
        }

        return null;

    }

    public void print() {

        System.out.println(description);

        for(DataSizeContainer container : dataSizeContainers) {
            container.print();
        }

    }

    public String getDescription() {
        return description;
    }

    @Override
    public Iterator<DataSizeContainer> iterator() {
        return dataSizeContainers.iterator();
    }
}
