The application is developed using Spring Boot.
https://spring.io/projects/spring-boot

To run in IDE:
Run the com.example.demo.DemoApplication main method as 'Spring Boot App' 
or Java Application'.

This class runs methods on Analytics.scala, and prints the output to the console.

The path and file names for the input files are defined as values at the top of Analytics.object.
These will need to be edited.

In order to work with other input files which have keys with different names, edit as follows:

To change the names of the keys in the VIDEO DATA files, modify the values of the following:
videoDataFileMinuteKeyLength
videoDataFileVideoKeyLength
videoDataFileDeviceIdKeyLength
videoDataFileWatchTimeKeyLength

(defined near the top of the scala class)

To change the names of the keys in the PROVIDERS file, modify the values of the following:
providersFileProviderKeyLength
providersFileVideoKeyLength

(defined in the method getVideoProviders())


