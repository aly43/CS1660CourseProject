# CS1660CourseProject

This project uses Docker and Mavin and Hadoop MapReduce Dependencies

To authenticate your GCP Storage, create a credentials.json and replace with the main method of App.java

The Jar files are to be uploaded to a GCP bucket and run through a Google Cluster

To run this program, replace all gcp buckets in the App.java to your own along with cluter id's
Through docker, build the image : docker build -t app .
Then you can run through docker as well : docker run --rm -it -e DISPLAY=$DISPLAY app
$DISPLAY will be your ip address, this will launch the gui 
