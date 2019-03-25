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
                    int duration, cores;
                    long dataSize;
                    String description;
                    String parts[] = line.split(" ");

                    cores = Integer.parseInt(parts[0]);
                    dataSize = Long.parseLong(parts[1]);
                    duration = (int) Double.parseDouble(parts[2]);
                    description = parts[3];

                    appInformation.add(new AppInformation(duration, cores, dataSize, description));
                }

                return appInformation;
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return null;

    }

}
