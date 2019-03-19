

import com.sun.deploy.ui.AppInfo;

import java.util.ArrayList;

public class InformationContainer {

    public ArrayList<AppInformation>[] coreAvgPerDS;

    public InformationContainer(String filepath) {

        loadInformation(filepath);

    }

    private void loadInformation(String filepath) {
        ArrayList<AppInformation> tmp = DataLoader.loadInformationFromFile(filepath);
        calculateAvgs(tmp);
    }

    private void calculateAvgs(ArrayList<AppInformation> info) {

        ArrayList<Long> dataSizes = new ArrayList<Long>();

        for(AppInformation appInfo : info) {
            if(!dataSizes.contains(appInfo.getDataSize())) {
                dataSizes.add(appInfo.getDataSize());
            }
        }

        //create separate value list for each data size
        ArrayList<AppInformation>[] orderedDSList = createLists(dataSizes.size());

        //add the AppInformation objects to the corresponding data size
        for(AppInformation appInfo : info) {
            long dataSize = appInfo.getDataSize();
            int i = 0;

            for(Long l : dataSizes) {
                if(l == dataSize) {
                    i = dataSizes.indexOf(l);
                    break;
                }
            }

            orderedDSList[i].add(appInfo);
        }

        for(int i = 0; i < orderedDSList.length; i++) {
            AppInformation[] orderedCores = orderByCores(orderedDSList[i].toArray(new AppInformation[orderedDSList[i].size()]));
            orderedDSList[i] = calculateCoreAvg(orderedCores);
        }

        for(int i = 0; i < orderedDSList.length; i++) {
            for(int j = 0; j < orderedDSList[i].size(); j++) {
                System.out.println(orderedDSList[i].get(j).toString());
            }
        }

        coreAvgPerDS = orderedDSList;

    }

    private ArrayList<AppInformation> calculateCoreAvg(AppInformation... obc) {

        ArrayList<AppInformation> avgCoreInfo = new ArrayList<AppInformation>();
        int count = 0, sum = 0, lastCores = 0;

        for(int i = 0; i < obc.length; i++) {
            if(i == 0) {
                lastCores = obc[i].getCores();
            } else if (i == obc.length - 1) {
                double avg = sum / (double) count;
                AppInformation val = new AppInformation(-1, lastCores, obc[i].getDataSize());
                val.setAvgTimeC(avg);
                avgCoreInfo.add(val);
            }

            if(lastCores == obc[i].getCores()) {
                sum += obc[i].getDuration();
                count++;
            } else {
                double avg = sum / (double) count;
                AppInformation val = new AppInformation(-1, lastCores, obc[i].getDataSize());
                val.setAvgTimeC(avg);
                avgCoreInfo.add(val);

                lastCores = obc[i].getCores();
                sum = obc[i].getDuration();
                count = 1;
            }
        }

        return avgCoreInfo;

    }

    private AppInformation[] orderByCores(AppInformation... info){

        for(int i = 1; i < info.length; i++) {
            for(int j = i; j > 0 && info[j].getCores() < info[j - 1].getCores(); j--) {
                AppInformation tmp = info[j - 1];
                info[j - 1] = info[j];
                info[j] = tmp;
            }
        }

        return info;

    }


    private <T> ArrayList<T>[] createLists(int size) {

        ArrayList<T>[] tmp = new ArrayList[size];

        for(int i = 0; i < size; i++) {
            tmp[i] = new ArrayList<T>();
        }

        return tmp;

    }

}
