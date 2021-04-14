package com.mycompany.app;

import java.awt.BorderLayout;
import java.awt.GridLayout;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.Dimension;
import java.time.Duration;
import java.time.Instant;

import javax.swing.*;
import java.lang.*;
import javax.swing.filechooser.*; 
import java.io.*;
import java.awt.*;

import java.io.FileInputStream;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.IOException;
import java.nio.channels.Channels;

import javax.swing.table.DefaultTableModel;

import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.dataproc.Dataproc;
import com.google.api.services.dataproc.DataprocScopes;
import com.google.api.services.dataproc.model.HadoopJob;
import com.google.api.services.dataproc.model.Job;
import com.google.api.services.dataproc.model.JobPlacement;
import com.google.api.services.dataproc.model.JobReference;
import com.google.api.services.dataproc.model.JobStatus;
import com.google.api.services.dataproc.model.SubmitJobRequest;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.UUID;

public class App {

	private static JFrame currFrame;
	private static JFrame frame;
	private static JFrame waitingforjob;
	private static JFrame selectAction;
	private static JFrame searchTop;
	private static JFrame searchTerm;
	private static JFrame displayTop;
	private static JFrame displayTerm;
	private static JLabel label2;
	private static File[] files;
	private static final String projectID = "aly43-project-option-2";
    private static final String bucketName = "aly43project";
    private static GoogleCredentials credential;
    private static Storage storage;
    private static String curJobId;
    private static String n;
    private static String term;
    private static String output;
    private static DefaultTableModel dataTable;
    private static DefaultTableModel dTable;

    public static void main(String[] args) throws IOException {

    	String[] colName = {"Frequency", "Term"};
    	dataTable = new DefaultTableModel(colName, 0);
    	String[] colNames = {"Frequency", "Term", "Document"};
    	dTable = new DefaultTableModel(colNames,0);
    	JButton goBack = new JButton("Back to select action");

        frame = new JFrame(); //initial frame
        currFrame = frame;
      	frame.setSize(500,500);
      	frame.setLocation(300,200);
      	frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
      	frame.setTitle("Alex Yang's Search Engine");

      	waitingforjob = new JFrame(); //frame acts a waiting screen
      	waitingforjob.setSize(500,500);
      	waitingforjob.setLocation(300,200);
      	waitingforjob.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
      	waitingforjob.setTitle("Waiting for Job...");

      	selectAction = new JFrame(); // new frame after mapreduce
      	selectAction.setSize(500, 500);
      	selectAction.setLocation(300,200);
      	selectAction.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
      	selectAction.setTitle("Choose an Operation");

      	searchTop = new JFrame(); //user input for top n page
      	searchTop.setSize(500, 500);
      	searchTop.setLocation(300,200);
      	searchTop.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
      	searchTop.setTitle("Search for top N");

      	searchTerm = new JFrame(); // search for term
      	searchTerm.setSize(500,500);
      	searchTerm.setLocation(300,200);
      	searchTerm.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
      	searchTerm.setTitle("Search for term");

      	displayTop = new JFrame(); // search for term
      	displayTop.setSize(500,500);
      	displayTop.setLocation(300,200);
      	displayTop.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
      	displayTop.setTitle("Results for Top N");

      	displayTerm = new JFrame(); // search for term
      	displayTerm.setSize(500,500);
      	displayTerm.setLocation(300,200);
      	displayTerm.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
      	displayTerm.setTitle("Results for Search");

      	JLabel loadPrompt = new JLabel("Load My Engine"); //part of initial frame
      	final JLabel label1 = new JLabel("<html>");
      	JButton button = new JButton("Choose Files");
      	JButton invert = new JButton("Construct Inverted Indicies");
      	final JPanel panel = new JPanel(); //part of inital frame
      	panel.setBorder(BorderFactory.createEmptyBorder(30, 30, 10, 30));
      	panel.setLayout(new GridLayout(0, 1));
      	panel.add(loadPrompt);
      	panel.add(button);
      	panel.add(label1);
      	panel.add(invert);
      	panel.add(goBack); 

      	JLabel headerWait = new JLabel("Waiting for Job"); // part of waiting screen
      	label2 = new JLabel("<html>");
      	JButton press = new JButton("Press");
      	final JPanel panel1 = new JPanel(); // part of waiting screen
      	panel1.setBorder(BorderFactory.createEmptyBorder(30, 30, 10, 30));
      	panel1.setLayout(new GridLayout(0, 1));
      	panel1.add(headerWait);
      	panel1.add(label2);
      	panel1.add(press);

      	JLabel headerAction = new JLabel("Job completed... Select action"); // part of algorithm selection
      	JButton searchForTerm = new JButton("Top-N");
      	JButton topN = new JButton("Search for Term");
      	final JPanel panel2 = new JPanel(); // part of algorithm selection
      	panel2.setBorder(BorderFactory.createEmptyBorder(30, 30, 10, 30));
      	panel2.setLayout(new GridLayout(0, 1));
      	panel2.add(headerAction);
      	panel2.add(searchForTerm);
      	panel2.add(topN);

      	JLabel topHeader = new JLabel("Enter your N value");
      	final JTextField field1 = new JTextField(10);
      	JButton enterN = new JButton("Search");
      	JButton goBack1 = new JButton("Back to select action");
      	final JPanel panel3 = new JPanel(); // search term
      	panel3.setBorder(BorderFactory.createEmptyBorder(30, 30, 10, 30));
      	panel3.setLayout(new GridLayout(0, 1));
      	panel3.add(topHeader);
      	panel3.add(field1);
      	panel3.add(enterN);
      	panel3.add(goBack1);

      	JLabel searchHeader = new JLabel("Enter a search term"); //search for term page
      	final JTextField field2 = new JTextField(20);
      	JButton enterTerm = new JButton("Search");
      	JButton goBack2 = new JButton("Back to select action");
      	final JPanel panel4 = new JPanel(); // search term
      	panel4.setBorder(BorderFactory.createEmptyBorder(30, 30, 10, 30));
      	panel4.setLayout(new GridLayout(0, 1));
      	panel4.add(searchHeader);
      	panel4.add(field2);
      	panel4.add(enterTerm);
      	panel4.add(goBack2);

      	JLabel topNResult = new JLabel("Top N Results");
      	final JTable topNTable = new JTable(dataTable);
      	topNTable.setBounds(250, 250, 500, 650); 
      	final JPanel panel5 = new JPanel();
      	JButton goBack3 = new JButton("Back to select action");
      	panel5.setBorder(BorderFactory.createEmptyBorder(30, 30, 10, 30));
      	panel5.setLayout(new GridLayout(0, 1));
      	panel5.add(topNResult);
      	panel5.add(topNTable);
      	panel5.add(goBack3);

      	JLabel searchResult = new JLabel("Search Result");
      	final JTable searchTable = new JTable(dTable);
      	searchTable.setBounds(250, 250, 500, 650); 
      	final JPanel panel6 = new JPanel();
      	JButton goBack4 = new JButton("Back to select action");
      	panel6.setBorder(BorderFactory.createEmptyBorder(30, 30, 10, 30));
      	panel6.setLayout(new GridLayout(0, 1));
      	panel6.add(searchResult);
      	panel6.add(searchTable);
      	panel6.add(goBack4);

      	frame.add(panel, BorderLayout.CENTER); // part of initial
      	frame.setVisible(true);

      	waitingforjob.add(panel1, BorderLayout.CENTER); // set frame to invisible
      	waitingforjob.setVisible(false);

      	selectAction.add(panel2, BorderLayout.CENTER);
      	selectAction.setVisible(false);

      	searchTop.add(panel3, BorderLayout.CENTER);
      	searchTop.setVisible(false);

      	searchTerm.add(panel4, BorderLayout.CENTER);
      	searchTerm.setVisible(false);

      	displayTop.add(panel5, BorderLayout.CENTER);
      	displayTop.setVisible(false);

      	displayTerm.add(panel6, BorderLayout.CENTER);
      	displayTerm.setVisible(false);

      	try {
        	// Storage
        	String fileName = "/usr/src/myapp/credentials.json";
			credential = GoogleCredentials.fromStream(new FileInputStream(fileName)).createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
			storage = StorageOptions.newBuilder().setCredentials(credential).build().getService();
            //storage = StorageOptions.getDefaultInstance().getService();
            System.out.println("Authorized storage successfully");
        } catch (Exception e) {
			System.out.println("Error gathering credentials:");
			System.out.println(e);
		}

      	//first button, loads in files
      	button.addActionListener(new ActionListener() {
          	@Override
          	public void actionPerformed(ActionEvent e) {

             	JFileChooser j = new JFileChooser();
              	j.setMultiSelectionEnabled(true);

              	int dialog = j.showSaveDialog(null);

              	if( dialog == JFileChooser.APPROVE_OPTION) {
                	files = j.getSelectedFiles();
                	for(File file : files) {
                  		System.out.println(file.getAbsolutePath());
                  		label1.setText(label1.getText() + "<br>" + file.getName());
                	}
              		label1.setText(label1.getText() + "</html>");
              	}
              	else {
                	System.out.println("No file selected...");
              	}
          	}
      	});

      	invert.addActionListener(new ActionListener() {
        	@Override
        	public void actionPerformed(ActionEvent e) {
        		try {
        			System.out.println(files.length);
        			if( files.length == 0)
	          		{
	           			App.infoBox("No files uploaded...", "Error");
	          		}
	          		else{
	          			curJobId = UUID.randomUUID().toString();

	          			//frame.setVisible(false);
	          			//waitingforjob.setVisible(true);

	            		uploadFiles();

	            		createJob();
	          		}
	          		System.out.println("Construct Inverted Indicies");
        		}
          		catch (Exception error) {
          			System.out.println("Something went wrong...");
          			System.out.println(error);
          		}
        	}
      	});

      	//test button
      	press.addActionListener(new ActionListener() {
      		@Override
      		public void actionPerformed(ActionEvent e) {
      			waitingforjob.setVisible(false);
      			selectAction.setVisible(true);
      		}
      	});

      	goBack.addActionListener(new ActionListener() { //go back button
    		@Override
    		public void actionPerformed(ActionEvent e) {
    			currFrame.setVisible(false);
    			selectAction.setVisible(true);
    		}
    	});
    	goBack1.addActionListener(new ActionListener() { //go back button
    		@Override
    		public void actionPerformed(ActionEvent e) {
    			currFrame.setVisible(false);
    			selectAction.setVisible(true);
    		}
    	});
    	goBack2.addActionListener(new ActionListener() { //go back button
    		@Override
    		public void actionPerformed(ActionEvent e) {
    			currFrame.setVisible(false);
    			selectAction.setVisible(true);
    		}
    	});
    	goBack3.addActionListener(new ActionListener() { //go back button
    		@Override
    		public void actionPerformed(ActionEvent e) {
    			currFrame.setVisible(false);
    			selectAction.setVisible(true);
    		}
    	});
    	goBack4.addActionListener(new ActionListener() { //go back button
    		@Override
    		public void actionPerformed(ActionEvent e) {
    			currFrame.setVisible(false);
    			selectAction.setVisible(true);
    		}
    	});

      	//buttons for algorithm screen
      	searchForTerm.addActionListener(new ActionListener() {
      		@Override
      		public void actionPerformed(ActionEvent e) {
      			System.out.println("Search for Term");
      			selectAction.setVisible(false);
      			searchTop.setVisible(true);
      			currFrame=searchTop;
      			//set search page to visible

      		}
      	});

      	topN.addActionListener(new ActionListener() {
      		@Override
      		public void actionPerformed(ActionEvent e) {
      			System.out.println("Display top N");
      			selectAction.setVisible(false);
      			searchTerm.setVisible(true);
      			currFrame=searchTerm;
      			//set topN page to visible
      		}
      	});

      	//search buttons
      	enterN.addActionListener(new ActionListener() {
      		@Override
      		public void actionPerformed(ActionEvent e) {
      			try {
      				if (field1.getText().isEmpty()) {
	      				System.out.println("Search field is empty");
	      			}
	      			else {
	      				n = field1.getText();
	      				System.out.println(n);
	      				createTopNJob();
	      			}
      			} catch (Exception err)
      			{
      				System.out.println(err);
      			}
      		}
      	});

      	enterTerm.addActionListener(new ActionListener() {
      		@Override
      		public void actionPerformed(ActionEvent e) {
      			if (field2.getText().isEmpty()){
      				System.out.println("Search field is empty");
      			}
      			else
      			{
      				System.out.println(field2.getText());
      				term = field2.getText();
      				createSearchJob();
      				//set search display to visible
      				// maybe wait for monitor job, after job done then display
      			}
      		}
      	});
    }

    private static void uploadFiles() throws IOException {
    	for(File file : files) {
    		//System.out.println("1");
    		BlobId blobId = BlobId.of(bucketName, "Data-" + curJobId + "/" + file.getName());
    		//System.out.println("2");
		    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
		    try {
		    		//System.out.println("3");
		            storage.create(blobInfo, Files.readAllBytes(Paths.get(file.getAbsolutePath())));
		            System.out.println("File " + file.getAbsolutePath() + " uploaded to bucket " + bucketName + " as " + file.getName());
		        } catch (Exception e) {
		            System.out.println("File " + file.getAbsolutePath() + " failed to upload to bucket " + bucketName + " as " + file.getName());
		            System.out.println(e);
		        }
    	}
    }

    private static void createJob() {
    	Job job = null;
        try {
            Dataproc dataproc = new Dataproc.Builder(new NetHttpTransport(), new JacksonFactory(),
                    new HttpCredentialsAdapter(credential)).setApplicationName("aly43 Project Option 2").build();
            job = dataproc.projects().regions().jobs().submit(projectID, "us-central1",
                    new SubmitJobRequest().setJob(new Job().setReference(new JobReference().setJobId("job-" + curJobId))
                            .setPlacement(new JobPlacement().setClusterName("cluster-151c"))
                            .setHadoopJob(new HadoopJob().setMainClass("InvertIndex").setJarFileUris(ImmutableList.of("gs://aly43project/InvertIndex.jar"))
                                    .setArgs(ImmutableList.of(
                                            "gs://aly43project/Data-" + curJobId,
                                            "gs://aly43project/output-" + curJobId)))))
                    .execute();
            output = "gs://aly43project/output-" + curJobId;
            System.out.println("Job sent successfully...");
            waitForJob(job, dataproc, curJobId);
        } catch (Exception e) {
            System.out.println("Job not sent successfully");
            System.out.println(e);
        }
    }

    private static void createTopNJob() {
    	Job job = null;
    	try {
            Dataproc dataproc = new Dataproc.Builder(new NetHttpTransport(), new JacksonFactory(),
                    new HttpCredentialsAdapter(credential)).setApplicationName("aly43 Project Option 2").build();
            job = dataproc.projects().regions().jobs().submit(projectID, "us-central1",
                    new SubmitJobRequest().setJob(new Job().setReference(new JobReference().setJobId("jobTop-" + curJobId))
                            .setPlacement(new JobPlacement().setClusterName("cluster-151c"))
                            .setHadoopJob(new HadoopJob().setMainClass("TopN").setJarFileUris(ImmutableList.of("gs://aly43project/TopN.jar"))
                                    .setArgs(ImmutableList.of(
                                            output + "/part-r-00000",
                                            "gs://aly43project/TopNoutput-" + curJobId,
                                            n)))))
                    .execute();
            System.out.println("Job sent successfully...");
            waitForTopNJob(job, dataproc, curJobId, "TopNoutput-" + curJobId);
        } catch (Exception e) {
            System.out.println("Job not sent successfully");
            System.out.println(e);
        }
    }

    private static void createSearchJob() {
    	Job job = null;
        try {
            Dataproc dataproc = new Dataproc.Builder(new NetHttpTransport(), new JacksonFactory(), new HttpCredentialsAdapter(credential)).setApplicationName("aly43 Project Option 2").build();
            job = dataproc.projects().regions().jobs().submit(projectID, "us-central1",
                    new SubmitJobRequest().setJob(new Job().setReference(new JobReference().setJobId("jobSearch-" + curJobId))
                            .setPlacement(new JobPlacement().setClusterName("cluster-151c"))
                            .setHadoopJob(new HadoopJob().setMainClass("Search").setJarFileUris(ImmutableList.of("gs://aly43project/Search.jar"))
                                    .setArgs(ImmutableList.of(
                                            output + "/part-r-00000",
                                            "gs://aly43project/SearchOutput-" + curJobId,
                                            term)))))
                    .execute();
            System.out.println("Job sent successfully...");
            waitForSearchJob(job, dataproc, curJobId, "SearchOutput-" + curJobId);
        } catch (Exception e) {
            System.out.println("Job not sent successfully");
            System.out.println(e);
        }
    }


    private static void waitForJob(Job job, Dataproc dataproc, String curJobId) {
    	frame.setVisible(false);
    	waitingforjob.setVisible(true);
    	Boolean running = true;

    	while(running) {
    		try {
    			job = dataproc.projects().regions().jobs().get(projectID, "us-central1", "job-" + curJobId).execute();
    			JobStatus status = job.getStatus();
    			label2.setText("Job is performing");
    			if (status.getState().compareTo("DONE") == 0) {
                    running = false;
                } else if (status.getState().compareTo("ERROR") == 0) {
                    running = false;
                }
    		} catch (Exception e) {
    			label2.setText("Problem with job");
    		}
    	}
    	label2.setText("Job completed, redirecting...");

    	waitingforjob.setVisible(false);
    	selectAction.setVisible(true);
    }

    private static void waitForTopNJob(Job job, Dataproc dataproc, String curJobId, String filePath) {
    	searchTop.setVisible(false);
    	waitingforjob.setVisible(true);
    	Boolean running = true;

    	while(running) {
    		try {
    			job = dataproc.projects().regions().jobs().get(projectID, "us-central1", "jobTop-" + curJobId).execute();
    			JobStatus status = job.getStatus();
    			label2.setText("Job is performing");
    			if (status.getState().compareTo("DONE") == 0) {
                    running = false;
                } else if (status.getState().compareTo("ERROR") == 0) {
                    running = false;
                }
    		} catch (Exception e) {
    			label2.setText("Problem with job");
    		}
    	}
    	label2.setText("Job completed, redirecting...");

    	waitingforjob.setVisible(false);
    	displayTop.setVisible(true);
    	displayResultsTop(filePath);
    }

    private static void waitForSearchJob(Job job, Dataproc dataproc, String curJobId, String filePath) {
    	searchTerm.setVisible(false);
    	waitingforjob.setVisible(true);
    	Boolean running = true;

    	while(running) {
    		try {
    			job = dataproc.projects().regions().jobs().get(projectID, "us-central1", "jobSearch-" + curJobId).execute();
    			JobStatus status = job.getStatus();
    			label2.setText("Job is performing");
    			if (status.getState().compareTo("DONE") == 0) {
                    running = false;
                } else if (status.getState().compareTo("ERROR") == 0) {
                    running = false;
                }
    		} catch (Exception e) {
    			label2.setText("Problem with job");
    		}
    	}
    	label2.setText("Job completed, redirecting...");

    	waitingforjob.setVisible(false);
    	displayTerm.setVisible(true);
    	displayResultsSearch(filePath);
    }

    private static void displayResultsTop(String filePath) {
    	currFrame = displayTop;
    	try{
    		File file = new File(filePath+".txt");
    		FileWriter fw = new FileWriter(file, true);
    		Blob blob = storage.get(BlobId.of(bucketName,  filePath + "/part-r-00000"));
    		ReadChannel readChannel = blob.reader();
            BufferedReader br = new BufferedReader(Channels.newReader(readChannel, "UTF-8"));
            BufferedWriter bw = new BufferedWriter(fw);
            String line = br.readLine();
            while(line != null)
            {
            	// Write to file
                bw.write(line);
                bw.newLine();
                // Add to data array
                String[] parts = line.split("\t");
                String freq = parts[0];
                String term = parts[1];
                dataTable.addRow(new String[]{freq, term});
                line = br.readLine();
            }
            bw.flush();
            bw.close();
    	} catch (Exception e){
    		System.out.println(e);
    	}
    }

    private static void displayResultsSearch(String filePath) {
    	currFrame = displayTerm;
    	try{
    		File file = new File(filePath+".txt");
    		FileWriter fw = new FileWriter(file, true);
    		Blob blob = storage.get(BlobId.of(bucketName,  filePath + "/part-r-00000"));
    		ReadChannel readChannel = blob.reader();
            BufferedReader br = new BufferedReader(Channels.newReader(readChannel, "UTF-8"));
            BufferedWriter bw = new BufferedWriter(fw);
            String line = br.readLine();
            while(line != null)
            {
            	// Write to file
                bw.write(line);
                bw.newLine();
                // Add to data array
                String[] parts = line.split("\t");
                String term = parts[0];
                String doc = parts[1];
                String freq = parts[2];
                dataTable.addRow(new String[]{term, doc, freq});
                line = br.readLine();
            }
            bw.flush();
            bw.close();
    	} catch (Exception e){
    		System.out.println(e);
    	}
    }
    	


    private static void infoBox(String infoMessage, String titleBar)
    {
        JOptionPane.showMessageDialog(null, infoMessage, "InfoBox: " + titleBar, JOptionPane.INFORMATION_MESSAGE);
    }
}