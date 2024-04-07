Instructions on how to run the project:
Extract the files required to run the system
Open the AWS folder in your desired IDE
Open the App class ---> Edit Run Configurations ---> Add new Run Configuration "App"
In program arguments provide the following:
	a."Extract Collocations" as the first argument.
	b.The minPmi value as the second argument.
	c.The minRelPmi value as the third argument.
Set the main class to "com.myproject.App"
Inside environment variables provide the AWS credentials to be able to access AWS services.
Once all of the above is completed, the system is ready to be run.
--------------------------------------------------------------------------------------------------------------------------------------------------------------------
On a local application, hit run (with the desired values for the arguments mentioned above) in the IDE you are using 
and the rest will be done by Amazon's Hadoop EMR platform.
After the system finishes running , The output files that contain the collocations for each decade sorted in decreasing order
will be stored in a folder named step5_output in our designated bucket.
--------------------------------------------------------------------------------------------------------------------------------------------------------------------

Dan Wiener,    209413640
Noa Youlzarie, 209320605