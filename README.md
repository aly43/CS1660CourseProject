# CS1660CourseProject

Criteria 	Implemented?

First Java Application Implementation and Execution on Docker 	Yes

Docker to Local (or GCP) Cluster Communication 	Yes

Inverted Indexing MapReduce Implementation and Execution on the Cluster (GCP) 	Yes

Term and Top-N Search 	Yes

Display the returned results of term search and Top-N in Jtable   Yes



This project uses Docker and Mavin and Hadoop MapReduce Dependencies

To authenticate your GCP Storage, create a credentials.json and replace within the main method of App.java
https://cloud.google.com/iam/docs/creating-managing-service-account-keys

The Jar files are to be uploaded to a GCP bucket and run through a Google Cluster

To run this program, replace all gcp buckets in the App.java to your own along with cluter id's
You will need to be runing Xming with your docker: https://docs.microsoft.com/en-us/archive/blogs/jamiedalton/windows-10-docker-gui for more information
Through docker, build the image : docker build -t app .
Then you can run through docker as well : docker run --rm -it -e DISPLAY=$DISPLAY app
$DISPLAY will be your ip address, this will launch the gui 
