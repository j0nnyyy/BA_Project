package core.containers;

import core.AppInformation;
import core.DataLoader;

import java.util.ArrayList;

public class DataContainer {

    private static DataContainer instance;
    private ArrayList<DescriptionContainer> descriptionContainers;

    private DataContainer() {
        descriptionContainers = new ArrayList<>();
    }

    public static DataContainer instance() {
        if(instance == null)
            instance = new DataContainer();
        return instance;
    }

    public void loadInformation(String filepath) {

        ArrayList<AppInformation> data = DataLoader.loadInformationFromFile(filepath);

        for(AppInformation info : data) {
            add(info);
        }

        sort();

    }

    public void add(AppInformation info) {

        for(DescriptionContainer container : descriptionContainers) {
            if(container.add(info)) {
                return;
            }
        }

        DescriptionContainer newContainer = new DescriptionContainer(info.getDescription());
        newContainer.add(info);
        descriptionContainers.add(newContainer);

    }

    public void sort() {

        for(DescriptionContainer container : descriptionContainers) {
            container.sort();
        }

    }

    public void print() {

        for(DescriptionContainer container : descriptionContainers) {
            container.print();
        }

    }

    public AppInformation getInformation(String description, long dataSize, int workers, int cores) {

        for(DescriptionContainer container : descriptionContainers) {
            if(container.getDescription().equals(description)) {
                return container.getInformation(dataSize, workers, cores);
            }
        }

        return null;

    }

    public ArrayList<AppInformation> getDSWorkerSeries(String description, long dataSize, int cores) {

        DescriptionContainer descContainer = null;
        DataSizeContainer dsContainer = null;
        ArrayList<AppInformation> infoList = new ArrayList<>();

        for(DescriptionContainer tmp : descriptionContainers) {
            if(tmp.getDescription().equals(description)) {
                descContainer = tmp;
                break;
            }
        }

        if(descContainer == null)
            return null;

        for(DataSizeContainer tmp : descContainer) {
            if(tmp.getDataSize() == dataSize) {
                dsContainer = tmp;
                break;
            }
        }

        if(dsContainer == null)
            return null;

        for(Integer i : dsContainer.getWorkerCounts()) {
            AppInformation info = dsContainer.getInformation(i, cores);
            if(info != null)
                infoList.add(info);
        }

        return infoList;

    }

    public ArrayList<AppInformation> getWorkerDSSeries(String description, int worker, int cores) {

        ArrayList<AppInformation> infoList = new ArrayList<>();
        DescriptionContainer descContainer = null;

        for(DescriptionContainer tmp : descriptionContainers) {
            if(tmp.getDescription().equals(description)) {
                descContainer = tmp;
                break;
            }
        }

        if(descContainer == null)
            return null;

        for(DataSizeContainer dsContainer : descContainer) {
            AppInformation info = dsContainer.getInformation(worker, cores);
            if(info != null)
                infoList.add(info);
        }

        return infoList;

    }

    public ArrayList<AppInformation> getDSCoreSeries(String description, long dataSize, int workers) {

        ArrayList<AppInformation> infoList = new ArrayList<>();
        DescriptionContainer descContainer = null;
        DataSizeContainer dsContainer = null;
        WorkerContainer wContainer = null;

        for(DescriptionContainer tmp : descriptionContainers) {
            if(tmp.getDescription().equals(description)) {
                descContainer = tmp;
                break;
            }
        }

        if(descContainer == null)
            return null;

        for(DataSizeContainer tmp : descContainer) {
            if(tmp.getDataSize() == dataSize) {
                dsContainer = tmp;
                break;
            }
        }

        if(dsContainer == null)
            return null;

        for(WorkerContainer tmp : dsContainer) {
            if(tmp.getWorkerCount() == workers) {
                wContainer = tmp;
                break;
            }
        }

        if(wContainer == null)
            return null;

        for(CoreContainer tmp : wContainer) {
            AppInformation info = tmp.getInformation();
            if(info != null)
                infoList.add(info);
        }

        return infoList;

    }

    public ArrayList<String> getDescriptions() {

        ArrayList<String> tmp = new ArrayList<>();

        for(DescriptionContainer container : descriptionContainers) {
            tmp.add(container.getDescription());
        }

        return tmp;

    }

    public ArrayList<Long> getDataSizes(String description) {

        for(DescriptionContainer container : descriptionContainers) {
            if(container.getDescription().equals(description)) {
                return container.getDataSizes();
            }
        }

        return null;

    }

    public ArrayList<Long> getCommonDataSizes(ArrayList<String> descriptions) {

        ArrayList<Long> commonDataSizes = getDataSizes(descriptions.get(0));

        for(int i = 1; i < descriptions.size(); i++) {
            ArrayList<Long> tmp = getDataSizes(descriptions.get(i));
            ArrayList<Long> toRemove = new ArrayList<>();

            for(Long l : commonDataSizes) {
                if(!tmp.contains(l)) {
                    toRemove.add(l);
                }
            }

            commonDataSizes.removeAll(toRemove);
        }

        return commonDataSizes;

    }

    public ArrayList<Integer> getAllWorkerCounts(String description) {

        ArrayList<Integer> workerCounts = new ArrayList<>();
        DescriptionContainer container = null;

        for(DescriptionContainer tmp : descriptionContainers) {
            if(tmp.getDescription().equals(description)) {
                container = tmp;
                break;
            }
        }

        if(container == null)
            return null;

        for(Long l : container.getDataSizes()) {
            for(Integer i : container.getWorkerCounts(l)) {
                if(!workerCounts.contains(i)) {
                    workerCounts.add(i);
                }
            }
        }

        return workerCounts;

    }

    public ArrayList<Integer> getCommonWorkerCounts(ArrayList<String> descriptions) {

        ArrayList<Integer> commonWorkerCounts = getAllWorkerCounts(descriptions.get(0));

        for(int i = 1; i < descriptions.size(); i++) {
            ArrayList<Integer> tmp = getAllWorkerCounts(descriptions.get(i));
            ArrayList<Integer> toRemove = new ArrayList<>();

            for(Integer j : commonWorkerCounts) {
                if(!tmp.contains(j)) {
                    toRemove.add(j);
                }
            }

            commonWorkerCounts.removeAll(toRemove);
        }

        return commonWorkerCounts;

    }

    public ArrayList<Integer> getWorkerCounts(String description, long datasize) {

        for(DescriptionContainer container : descriptionContainers) {
            if(container.getDescription().equals(description)) {
                return container.getWorkerCounts(datasize);
            }
        }

        return null;

    }

    public ArrayList<Integer> getCoreCounts(String description, long datasize, int workerCount) {

        for(DescriptionContainer container : descriptionContainers) {
            if(container.getDescription().equals(description)) {
                return container.getCoreCounts(datasize, workerCount);
            }
        }

        return null;

    }

    public ArrayList<Integer> getAllCoreCounts(String description) {

        ArrayList<Integer> coreCounts = new ArrayList<>();
        DescriptionContainer container = null;

        for(DescriptionContainer tmp : descriptionContainers) {
            if(tmp.getDescription().equals(description)) {
                container = tmp;
                break;
            }
        }

        if(container == null)
            return null;

        for(Long l : container.getDataSizes()) {
            for(Integer i : container.getWorkerCounts(l)) {
                for(Integer j : container.getCoreCounts(l, i)) {
                    if(!coreCounts.contains(j)) {
                        coreCounts.add(j);
                    }
                }
            }
        }

        return coreCounts;

    }

    public ArrayList<Integer> getCommonCoreCounts(ArrayList<String> descriptions) {

        ArrayList<Integer> commonCoreCounts = getAllCoreCounts(descriptions.get(0));

        for(int i = 1; i < descriptions.size(); i++) {
            ArrayList<Integer> tmp = getAllCoreCounts(descriptions.get(i));
            ArrayList<Integer> toRemove = new ArrayList<>();

            for(Integer j : commonCoreCounts) {
                if(!tmp.contains(j)) {
                    toRemove.add(j);
                }
            }

            commonCoreCounts.removeAll(toRemove);
        }

        return commonCoreCounts;

    }

    public static void main(String[] args) {

        DataContainer container = DataContainer.instance();
        container.loadInformation("C:/Users/Jonas/Desktop/BA_Project/newlog.txt");
        container.print();

    }

}
