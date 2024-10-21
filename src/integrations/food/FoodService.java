package integrations.food;

import haven.Defer;
import haven.ItemInfo;
import haven.Resource;
import haven.Utils;
import haven.res.ui.tt.q.qbuff.QBuff;
import haven.resutil.FoodInfo;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FoodService {
    public static final String API_ENDPOINT = "http://localhost:3000/";
    public static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private static final String PROJECT_PATH = System.getProperty("user.dir") + "/FoodData/";  // Project directory path
    private static final File FOOD_DATA_CACHE_FILE = new File(PROJECT_PATH + "food_data.json");

    static {
        if (!Resource.language.equals("en")) {
            System.out.println("FoodUtil ERROR: Only English language is allowed to send food data");
        }
        scheduler.execute(FoodService::checkFood);
        scheduler.scheduleAtFixedRate(FoodService::saveFoodDataToFile, 10L, 10, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(FoodService::runPythonScript, 0L, 30, TimeUnit.MINUTES);
    }

    /**
     * Check item info and determine if it is food and we need to send it
     */
    public static void checkFood(List<ItemInfo> ii, Resource res) {
        if (!Resource.language.equals("en")) {
            // Do not process localized items
            return;
        }
        List<ItemInfo> infoList = new ArrayList<>(ii);
        new Thread(() -> {
            try {
                String resName = res.name;
                FoodInfo foodInfo = ItemInfo.find(FoodInfo.class, infoList);
                if (foodInfo != null) {
                    QBuff qBuff = ItemInfo.find(QBuff.class, infoList);
                    double quality = qBuff != null ? qBuff.q : 10.0;
                    double multiplier = Math.sqrt(quality / 10.0);

                    ParsedFoodInfo parsedFoodInfo = new ParsedFoodInfo();
                    parsedFoodInfo.resourceName = resName;
                    parsedFoodInfo.energy = (int) Math.round(foodInfo.end * 100);  // Correctly cast to int
                    parsedFoodInfo.hunger = foodInfo.glut; // Store as double

                    for (int i = 0; i < foodInfo.evs.length; i++) {
                        parsedFoodInfo.feps.add(new FoodFEP(foodInfo.evs[i].ev.orignm, round2Dig(foodInfo.evs[i].a / multiplier)));
                    }

                    for (ItemInfo info : infoList) {
                        if (info instanceof ItemInfo.AdHoc) {
                            String text = ((ItemInfo.AdHoc) info).str.text;
                            // Skip food which base FEP's cannot be calculated
                            if (text.equals("White-truffled") || text.equals("Black-truffled") || text.equals("Peppered")) {
                                return;
                            }
                        }
                        if (info instanceof ItemInfo.Name) {
                            parsedFoodInfo.itemName = ((ItemInfo.Name) info).ostr.text;
                        }
                        if (info.getClass().getName().equals("Ingredient")) {
                            String name = (String) info.getClass().getField("oname").get(info);
                            Double value = (Double) info.getClass().getField("val").get(info);
                            parsedFoodInfo.ingredients.add(new FoodIngredient(name, (int) (value * 100)));
                        } else if (info.getClass().getName().equals("Smoke")) {
                            String name = (String) info.getClass().getField("oname").get(info);
                            Double value = (Double) info.getClass().getField("val").get(info);
                            parsedFoodInfo.ingredients.add(new FoodIngredient(name, (int) (value * 100)));
                        }
                    }

                    // Save parsed food information to file
                    saveFoodDataToFile("food_data.json", parsedFoodInfo);
                }
            } catch (Exception ex) {
                System.out.println("Cannot create food info: " + ex.getMessage());
            }
        }).start();
    }

    private static double round2Dig(double value) {
        return Math.round(value * 100.0) / 100.0;
    }

    public static void saveFoodDataToFile(String filename, ParsedFoodInfo parsedFoodInfo) {
        try {
            File file = FOOD_DATA_CACHE_FILE;
            JSONArray jsonArray;
            if (file.exists()) {
                // If the file exists, read existing data and append
                String existingData = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
                jsonArray = new JSONArray(existingData);
            } else {
                // If the file doesn't exist, create a new JSON array
                jsonArray = new JSONArray();
            }

            // Create JSON object for the new food info
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("itemName", parsedFoodInfo.getItemName());
            jsonObject.put("resourceName", parsedFoodInfo.getResourceName());
            jsonObject.put("energy", parsedFoodInfo.getEnergy());
            jsonObject.put("hunger", parsedFoodInfo.getHunger()); // Save as double

            JSONArray ingredientsArray = new JSONArray();
            for (FoodIngredient ingredient : parsedFoodInfo.getIngredients()) {
                JSONObject ingredientObject = new JSONObject();
                ingredientObject.put("name", ingredient.getName());
                ingredientObject.put("percentage", ingredient.getPercentage());
                ingredientsArray.put(ingredientObject);
            }
            jsonObject.put("ingredients", ingredientsArray);

            JSONArray fepsArray = new JSONArray();
            for (FoodFEP fep : parsedFoodInfo.getFeps()) {
                JSONObject fepObject = new JSONObject();
                fepObject.put("name", fep.getName());
                fepObject.put("value", fep.getValue());
                fepsArray.put(fepObject);
            }
            jsonObject.put("feps", fepsArray);

            // Append the new food info to the existing array
            jsonArray.put(jsonObject);

            // Write the updated array back to the file
            Files.write(file.toPath(), Collections.singleton(jsonArray.toString(4)), StandardCharsets.UTF_8, StandardOpenOption.CREATE);
            System.out.println("Food data saved to: " + file.getAbsolutePath());

            // Run the Python script
            runPythonScript("ImportFoodDataFromDesktopToSQLITE.py"); // Use your script's name
        } catch (IOException e) {
            System.err.println("Error saving food data: " + e.getMessage());
        }
    }

    private static void runPythonScript(String scriptName) {
        try {
            // Full path to the Python executable
            String pythonPath = System.getProperty("user.dir") + "/bin/python/python.exe"; // Replace with the actual path to your python.exe
            // Full path to your script
            String scriptPath = System.getProperty("user.dir") + "/ImportFoodDataFromDesktopToSQLITE.py"; // Ensure this points to the correct location of the script

            // Create a process builder
            ProcessBuilder processBuilder = new ProcessBuilder(pythonPath, scriptPath);
            processBuilder.redirectErrorStream(true); // Redirect error stream to output stream

            // Start the process
            Process process = processBuilder.start();

            // Capture output
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println(line); // Print the output of the script
                }
            }

            // Wait for the process to finish
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                System.err.println("Python script exited with code: " + exitCode);
            }
        } catch (IOException | InterruptedException e) {
            System.err.println("Error running Python script: " + e.getMessage());
        }
    }

    public static class FoodIngredient {
        private String name;
        private Integer percentage;

        public FoodIngredient(String name, Integer percentage) {
            this.name = name;
            this.percentage = percentage;
        }

        public Integer getPercentage() {
            return percentage;
        }

        public String getName() {
            return name;
        }
    }

    public static class FoodFEP {
        private String name;
        private Double value;

        public FoodFEP(String name, Double value) {
            this.name = name;
            this.value = value;
        }

        public String getName() {
            return name;
        }

        public Double getValue() {
            return value;
        }
    }

    public static class ParsedFoodInfo {
        private String itemName;
        private String resourceName;
        private int energy;   // This should be an int
        private double hunger; // Change to double
        private List<FoodIngredient> ingredients = new ArrayList<>();
        private List<FoodFEP> feps = new ArrayList<>();

        public String getItemName() {
            return itemName;
        }

        public String getResourceName() {
            return resourceName;
        }

        public int getEnergy() {
            return energy;
        }

        public double getHunger() {
            return hunger; // Return as double
        }

        public List<FoodIngredient> getIngredients() {
            return ingredients;
        }

        public List<FoodFEP> getFeps() {
            return feps;
        }
    }
}