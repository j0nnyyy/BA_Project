package core;

import java.util.ArrayList;
import java.util.Arrays;

public class InformationContainer {

    public ArrayList<ArrayList<ArrayList<AppInformation>>> descriptionList;

    public InformationContainer(String filepath) {

        loadInformation(filepath);

    }

    private void loadInformation(String filepath) {

        ArrayList<ArrayList<AppInformation>> orderedDescriptions = orderByDescription(DataLoader.loadInformationFromFile(filepath));
        ArrayList<ArrayList<ArrayList<AppInformation>>> orderedCores = new ArrayList<>();
        descriptionList = new ArrayList<>();

        for(ArrayList<AppInformation> singleSeries : orderedDescriptions) {
            descriptionList.add(orderByFileCount(singleSeries));
        }

        for(int i = 0; i < descriptionList.size(); i++) {
            ArrayList<ArrayList<AppInformation>> tmp = new ArrayList<>();
            for(int j = 0; j < descriptionList.get(i).size(); j++) {
                tmp.add(orderByCores(descriptionList.get(i).get(j)));
            }
            orderedCores.add(tmp);
        }

        descriptionList = orderedCores;

    }

    private ArrayList<ArrayList<AppInformation>> orderByDescription(ArrayList<AppInformation> appInformation) {

        ArrayList<ArrayList<AppInformation>> tmp = new ArrayList<>();

        for(AppInformation info : appInformation) {
            boolean found = false;

            for(ArrayList<AppInformation> type : tmp) {
                if(type.get(0).getDescription().equals(info.getDescription())) {
                    found = true;
                    type.add(info);
                }
            }

            if(!found) {
                tmp.add(new ArrayList<>());
                tmp.get(tmp.size() - 1).add(info);
            }
        }

        return tmp;

    }

    private ArrayList<ArrayList<AppInformation>> orderByFileCount(ArrayList<AppInformation> appInformation) {

        ArrayList<ArrayList<AppInformation>> orderedDSList = new ArrayList<>();

        for(AppInformation info : appInformation) {
            boolean found = false;

            for(ArrayList<AppInformation> dsList : orderedDSList) {
                if(dsList.get(0).getDataSize() == info.getDataSize()) {
                    found = true;
                    dsList.add(info);
                }
            }

            if(!found) {
                orderedDSList.add(new ArrayList<>());
                orderedDSList.get(orderedDSList.size() - 1).add(info);
            }
        }

        return orderedDSList;

    }

    private ArrayList<AppInformation> orderByCores(ArrayList<AppInformation> appInformation) {

        AppInformation[] infoArray = appInformation.toArray(new AppInformation[appInformation.size()]);

        for(int i = 1; i < infoArray.length; i++) {
            for(int j = i; j > 0 && infoArray[j].getCores() < infoArray[j - 1].getCores(); j--) {
                AppInformation tmp = infoArray[j - 1];
                infoArray[j - 1] = infoArray[j];
                infoArray[j] = tmp;
            }
        }

        return new ArrayList(Arrays.asList(infoArray));

    }

    public ArrayList<String> getSeriesNames() {

        ArrayList<String> sNames = new ArrayList<>();

        for(int i = 0; i < descriptionList.size(); i++) {
            if(!sNames.contains(descriptionList.get(i).get(0).get(0).getDescription())) {
                sNames.add(descriptionList.get(i).get(0).get(0).getDescription());
            }
        }

        return sNames;

    }

    public ArrayList<Long> getCommonFileCounts(ArrayList<String> series) {

        ArrayList<Long> commonFileCounts = new ArrayList<>();
        boolean first = true;

        for(ArrayList<ArrayList<AppInformation>> description : descriptionList)
            if(series.contains(description.get(0).get(0).getDescription())) {
                if(first) {
                    first = false;
                    for(ArrayList<AppInformation> dsList : description) {
                        commonFileCounts.add(dsList.get(0).getDataSize());
                    }
                } else {
                    ArrayList<Long> toRemove = new ArrayList<>();

                    for(Long l : commonFileCounts) {
                        boolean contained = false;

                        for(ArrayList<AppInformation> dsList : description) {
                            if(dsList.get(0).getDataSize() == l) {
                                contained = true;
                                break;
                            }
                        }

                        if(!contained) {
                            toRemove.add(l);
                        }
                    }

                    commonFileCounts.removeAll(toRemove);
                }
            }

        return commonFileCounts;

    }

    public ArrayList<ArrayList<AppInformation>> getSeriesByDescription(String description) {

        for(ArrayList<ArrayList<AppInformation>> series : descriptionList) {
            if(series.get(0).get(0).getDescription().equals(description)) {
                return series;
            }
        }

        return null;

    }

    public void printContainer() {

        for(ArrayList<ArrayList<AppInformation>> description : descriptionList) {
            System.out.println(description.get(0).get(0).getDescription());
            for(ArrayList<AppInformation> dsList : description) {
                System.out.println(dsList.get(0).getDataSize());
                for(AppInformation info : dsList) {
                    System.out.println(info.getCores());
                }
            }
        }

    }

}
