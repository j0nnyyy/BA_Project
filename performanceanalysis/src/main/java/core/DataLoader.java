package core;

import java.io.*;
import java.util.ArrayList;

public class DataLoader {

    public static ArrayList<AppInformation> loadInformationFromFile(String filepath) {

        File f = new File(filepath);

        if(f.exists()) {
            try {
                ArrayList<AppInformation> appInformation = new ArrayList<AppInformation>();
                BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
                String line;

                while((line = reader.readLine()) != null) {
                    if(line.startsWith("#"))
                        continue;

                    int workers, cores;
                    double duration;
                    long dataSize;
                    String description;
                    String parts[] = line.split(" ");

                    workers = Integer.parseInt(parts[0]);
                    cores = Integer.parseInt(parts[1]);
                    dataSize = Long.parseLong(parts[2]);
                    duration = Double.parseDouble(parts[3]);
                    description = parts[4];

                    boolean found = false;

                    for(AppInformation info : appInformation) {
                        if(info.getDescription().equals(description)
                                && info.getDataSize() == dataSize
                                && info.getWorkers() == workers
                                && info.getCores() == cores) {
                            found = true;
                            info.addDuration(duration);
                        }
                    }

                    if(!found) {
                        AppInformation info = new AppInformation(workers, cores, dataSize, description);
                        info.addDuration(duration);
                        appInformation.add(info);
                    }
                }

                return appInformation;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return null;

    }

}
