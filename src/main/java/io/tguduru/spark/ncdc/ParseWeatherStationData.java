package io.tguduru.spark.ncdc;

import io.tguduru.spark.ncdc.model.WeatherStation;
import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An utility which parses the weather station data.
 *
 * @author Guduru, Thirupathi Reddy
 * @modified 3/11/17
 */
public class ParseWeatherStationData {
    static Logger logger = Logger.getLogger(ParseWeatherStationData.class.getName());

    public static void main(String[] args) throws IOException {
        InputStreamReader inputStream = new InputStreamReader(ParseWeatherStationData.class.getClassLoader().getResourceAsStream("weather_station_list.psv"));
        BufferedReader bufferedReader = new BufferedReader(inputStream);
        String line = null;

        while ((line = bufferedReader.readLine()) != null) {
            parseWeatherRecord(line);
            break;
        }

    }

    public static void parseWeatherRecord(String line) {
        WeatherStation weatherStation = new WeatherStation();
        String[] weatherStationRecord = StringUtils.splitPreserveAllTokens(line, '|');
        // System.out.println(weatherStationRecord.length);

        if (weatherStationRecord.length != 19) {
            logger.log(Level.SEVERE, " Invalid Record :  " + line);
        }
        weatherStation.setRegion(StringUtils.substringBetween(weatherStationRecord[0], "\"", "\""));
        weatherStation.setCode(StringUtils.substringBetween(weatherStationRecord[1], "\"", "\""));
        weatherStation.setName(StringUtils.substringBetween(weatherStationRecord[2], "\"", "\""));
        weatherStation.setState(StringUtils.substringBetween(weatherStationRecord[3], "\"", "\""));
        weatherStation.setCounty(StringUtils.substringBetween(weatherStationRecord[4], "\"", "\""));
        weatherStation.setCountry(StringUtils.substringBetween(weatherStationRecord[5], "\"", "\""));
        weatherStation.setExtendedName(StringUtils.substringBetween(weatherStationRecord[6], "\"", "\""));
        weatherStation.setCallSign(StringUtils.substringBetween(weatherStationRecord[7], "\"", "\""));
        weatherStation.setStationType(StringUtils.substringBetween(weatherStationRecord[8], "\"", "\""));
        weatherStation.setAssignedDate(StringUtils.substringBetween(weatherStationRecord[9], "\"", "\""));
        weatherStation.setStateDate(StringUtils.substringBetween(weatherStationRecord[10], "\"", "\""));
        weatherStation.setComments(StringUtils.substringBetween(weatherStationRecord[11], "\"", "\""));
        weatherStation.setGeoLocation(StringUtils.substringBetween(weatherStationRecord[12], "\"", "\""));
        weatherStation.setOtherElevation(StringUtils.substringBetween(weatherStationRecord[13], "\"", "\""));
        weatherStation.setGroundElevation(StringUtils.substringBetween(weatherStationRecord[14], "\"", "\""));
        weatherStation.setRunwayElevation(StringUtils.substringBetween(weatherStationRecord[15], "\"", "\""));
        weatherStation.setBarometricElevation(StringUtils.substringBetween(weatherStationRecord[16], "\"", "\""));
        weatherStation.setStationElevation(StringUtils.substringBetween(weatherStationRecord[17], "\"", "\""));
        weatherStation.setUpperAirElevation(StringUtils.substringBetween(weatherStationRecord[18], "\"", "\""));

        System.out.println(weatherStation);
    }
}
