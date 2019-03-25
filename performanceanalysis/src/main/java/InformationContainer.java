import com.sun.deploy.ui.AppInfo;

import java.util.ArrayList;

public class InformationContainer {

    public ArrayList<ArrayList<ArrayList<AppInformation>>> coreAvgPerDescDs;

    public InformationContainer(String filepath) {

        loadInformation(filepath);

    }

    private void loadInformation(String filepath) {

        coreAvgPerDescDs = calculateAverages(orderByDatasize(orderByDescription(DataLoader.loadInformationFromFile(filepath))));

    }

    private ArrayList<ArrayList<AppInformation>> orderByDescription(ArrayList<AppInformation> appInformation) {

        ArrayList<ArrayList<AppInformation>> orderedDescriptions = new ArrayList<ArrayList<AppInformation>>();

        for(AppInformation info : appInformation) {
            int listIndex = -1;
            for(int i = 0; i < orderedDescriptions.size(); i++) {
                ArrayList<AppInformation> list = orderedDescriptions.get(i);
                if(list.get(0).getDescription().equals(info.getDescription())) {
                    listIndex = i;
                    break;
                }
            }

            if(listIndex != -1) {
                orderedDescriptions.get(listIndex).add(info);
            } else {
                orderedDescriptions.add(new ArrayList<AppInformation>());
                orderedDescriptions.get(orderedDescriptions.size() - 1).add(info);
            }
        }

        return orderedDescriptions;

    }

    private ArrayList<ArrayList<ArrayList<AppInformation>>> orderByDatasize(ArrayList<ArrayList<AppInformation>> appInformation) {

        ArrayList<ArrayList<ArrayList<AppInformation>>> orderedDatasizes = new ArrayList<ArrayList<ArrayList<AppInformation>>>();

        for(ArrayList<AppInformation> infoList : appInformation) {
            orderedDatasizes.add(new ArrayList<ArrayList<AppInformation>>());

            for(AppInformation val : infoList) {
                int listIndex = -1;

                for(int i = 0; i < orderedDatasizes.get(orderedDatasizes.size() - 1).size(); i++) {
                    ArrayList<AppInformation> tmp = orderedDatasizes.get(orderedDatasizes.size() - 1).get(i);
                    if(tmp.get(0).getDataSize() == val.getDataSize()) {
                        listIndex = i;
                        break;
                    }
                }

                if(listIndex != -1) {
                    orderedDatasizes.get(orderedDatasizes.size() - 1).get(listIndex).add(val);
                } else {
                    ArrayList<ArrayList<AppInformation>> tmp = orderedDatasizes.get(orderedDatasizes.size() - 1);
                    tmp.add(new ArrayList<AppInformation>());
                    tmp.get(tmp.size() - 1).add(val);
                }
            }
        }

        return orderedDatasizes;

    }

    private ArrayList<ArrayList<ArrayList<AppInformation>>> calculateAverages(ArrayList<ArrayList<ArrayList<AppInformation>>> orderedLists) {

        ArrayList<ArrayList<ArrayList<AppInformation>>> avgs = new ArrayList<ArrayList<ArrayList<AppInformation>>>();

        for(ArrayList<ArrayList<AppInformation>> dsLists : orderedLists) {
            avgs.add(new ArrayList<ArrayList<AppInformation>>());

            for(ArrayList<AppInformation> infoList : dsLists) {
                avgs.get(avgs.size() - 1).add(calculateCoreAvg(infoList.toArray(new AppInformation[infoList.size()])));
            }
        }

        return avgs;

    }

    private ArrayList<AppInformation> calculateCoreAvg(AppInformation... obc) {

        ArrayList<AppInformation> avgCoreInfo = new ArrayList<AppInformation>();
        int count = 0, sum = 0, lastCores = 0;

        for(int i = 0; i < obc.length; i++) {
            if(i == 0) {
                lastCores = obc[i].getCores();
            }

            if(lastCores == obc[i].getCores()) {
                sum += obc[i].getDuration();
                count++;
            } else {
                double avg = sum / (double) count;
                AppInformation val = new AppInformation(-1, lastCores, obc[i].getDataSize(), obc[i].getDescription());
                val.setAvgTimeC(avg);
                avgCoreInfo.add(val);

                lastCores = obc[i].getCores();
                sum = obc[i].getDuration();
                count = 1;
            }

            if (i == obc.length - 1) {
                double avg = sum / (double) count;
                AppInformation val = new AppInformation(-1, lastCores, obc[i].getDataSize(), obc[i].getDescription());
                val.setAvgTimeC(avg);
                avgCoreInfo.add(val);
            }
        }

        return avgCoreInfo;

    }

}
