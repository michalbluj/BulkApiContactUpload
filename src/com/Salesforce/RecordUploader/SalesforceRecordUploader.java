package com.Salesforce.RecordUploader;
import com.sforce.async.*;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

import java.io.*;
import java.util.*;

/**
 * Salesforce uploader of records in CSV format utilizing Salesforce Batch API
 *
 * @author Cezary Zeleznicki
 * @date 11.08.17
 */
public class SalesforceRecordUploader {
    private static String sfEndpoint;

    public static void main(String[] args) throws AsyncApiException, ConnectionException, IOException {

        SalesforceRecordUploader scu = new SalesforceRecordUploader();
        Properties prop = scu.readProperties();
        sfEndpoint = prop.getProperty("sf_endpoint");

        scu.run(prop.getProperty("sobjectType"), prop.getProperty("username"), prop.getProperty("password"), prop.getProperty("filename"));
    }

    /**
     * Reads configuration file with credentials and filename to process
     * @return Properties instance
     */
    private Properties readProperties() {
        Properties prop = new Properties();
        InputStream input = null;

        try {
            input = new FileInputStream("config.properties");
            prop.load(input);
        }catch (FileNotFoundException ex) {
            System.out.println("config.properties file not found!");
            System.exit(1);
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            if (input == null) {
                return prop;
            }

            try {
                input.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        List<String> missingKeys = new ArrayList<String>();
        if(!prop.containsKey("sobjectType")) {
            missingKeys.add("sobjectType");
        }

        if(!prop.containsKey("username")) {
            missingKeys.add("username");
        }

        if(!prop.containsKey("password")) {
            missingKeys.add("password");
        }

        if(!prop.containsKey("filename")) {
            missingKeys.add("filename");
        }

        if(!prop.containsKey("sf_endpoint")) {
            missingKeys.add("sf_endpoint");
        }

        if(!missingKeys.isEmpty()) {
            System.out.println("Please check config file, following properties are missing: " +
                    String.join(", ", missingKeys));
            System.exit(1);
        }

        return prop;
    }

    /**
     * Creates a Bulk API job and uploads batches for a CSV file.
     */
    public void run(String sobjectType, String userName, String password, String fileName)
            throws AsyncApiException, ConnectionException, IOException {

        BulkConnection connection = getBulkConnection(userName, password);
        JobInfo job = createJob(sobjectType, connection);
        List<BatchInfo> batchInfoList = createBatchesFromCSVFile(connection, job, fileName);

        closeJob(connection, job.getId());
        awaitCompletion(connection, job, batchInfoList);
        checkResults(connection, job, batchInfoList);
    }


    /**
     * Gets the results of the operation and checks for errors.
     */
    private void checkResults(BulkConnection connection, JobInfo job, List<BatchInfo> batchInfoList)
        throws AsyncApiException, IOException {

        for (BatchInfo b : batchInfoList) {
            CSVReader rdr = new CSVReader(connection.getBatchResultStream(job.getId(), b.getId()));
            List<String> resultHeader = rdr.nextRecord();
            int resultCols = resultHeader.size();

            List<String> row;
            while ((row = rdr.nextRecord()) != null) {
                Map<String, String> resultInfo = new HashMap<String, String>();
                for (int i = 0; i < resultCols; i++) {
                    resultInfo.put(resultHeader.get(i), row.get(i));
                }

                boolean success = Boolean.valueOf(resultInfo.get("Success"));
                boolean created = Boolean.valueOf(resultInfo.get("Created"));

                String id = resultInfo.get("Id");
                String error = resultInfo.get("Error");

                if (success && created) {
                    System.out.println("Created row with id " + id);
                } else if (!success) {
                    System.out.println("Failed with error: " + error);
                }
            }
        }
    }

    private void closeJob(BulkConnection connection, String jobId)
        throws AsyncApiException {

        JobInfo job = new JobInfo();
        job.setId(jobId);
        job.setState(JobStateEnum.Closed);
        connection.updateJob(job);
    }

    /**
     * Wait for a job to complete by polling the Bulk API.
     *
     * @param connection BulkConnection used to check results.
     * @param job The job awaiting completion.
     * @param batchInfoList List of batches for this job.
     * @throws AsyncApiException
     */
    private void awaitCompletion(BulkConnection connection, JobInfo job, List<BatchInfo> batchInfoList)
        throws AsyncApiException {

        long totalProcessingTime = 0L;
        long apiActiveProcessingTime = 0L;
        long apexProcessingTime = 0L;
        long sleepTime = 0L;

        Set<String> incomplete = new HashSet<String>();
        for (BatchInfo bi : batchInfoList) {
            incomplete.add(bi.getId());
        }

        while (!incomplete.isEmpty()) {
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {}

            System.out.println("Awaiting results..." + incomplete.size());
            sleepTime = 10000L;
            BatchInfo[] statusList = connection.getBatchInfoList(job.getId()).getBatchInfo();

            for (BatchInfo b : statusList) {
                if (b.getState() != BatchStateEnum.Completed && b.getState() != BatchStateEnum.Failed) {
                    continue;
                }

                if (incomplete.remove(b.getId())) {
                    totalProcessingTime += b.getTotalProcessingTime();
                    apiActiveProcessingTime += b.getApiActiveProcessingTime();
                    apexProcessingTime += b.getApexProcessingTime();

                    System.out.println("BATCH STATUS:\n" + b);
                }
            }
        }

        System.out.println("Total Processing Time: " + totalProcessingTime);
        System.out.println("Total API Active Processing Time: " + apiActiveProcessingTime);
        System.out.println("Total Apex Time: " + apexProcessingTime + "\n");
    }

    /**
     * Create a new job using the Bulk API.
     *
     * @param sobjectType The object type being loaded, such as "Account"
     * @param connection BulkConnection used to create the new job.
     * @return The JobInfo for the new job.
     * @throws AsyncApiException
     */
    private JobInfo createJob(String sobjectType, BulkConnection connection) throws AsyncApiException {
        JobInfo job = new JobInfo();
        job.setObject(sobjectType);
        job.setOperation(OperationEnum.insert);
        job.setContentType(ContentType.CSV);
        job = connection.createJob(job);

        System.out.println(job);
        return job;
    }

    /**
     * Create the BulkConnection used to call Bulk API operations.
     */
    private BulkConnection getBulkConnection(String userName, String password)
        throws ConnectionException, AsyncApiException {

        ConnectorConfig partnerConfig = new ConnectorConfig();
        partnerConfig.setUsername(userName);
        partnerConfig.setPassword(password);
        partnerConfig.setAuthEndpoint(sfEndpoint);

        new PartnerConnection(partnerConfig);

        ConnectorConfig config = new ConnectorConfig();
        config.setSessionId(partnerConfig.getSessionId());

        String soapEndpoint = partnerConfig.getServiceEndpoint();
        String apiVersion = "40.0";
        String restEndpoint = soapEndpoint.substring(0, soapEndpoint.indexOf("Soap/")) + "async/" + apiVersion;
        config.setRestEndpoint(restEndpoint);

        config.setCompression(true);

        config.setTraceMessage(false);
        BulkConnection connection = new BulkConnection(config);
        return connection;
    }

    /**
     * Create and upload batches using a CSV file.
     * The file into the appropriate size batch files.
     *
     * @param connection Connection to use for creating batches
     * @param jobInfo Job associated with new batches
     * @param csvFileName The source file for batch data
     */
    private List<BatchInfo> createBatchesFromCSVFile(BulkConnection connection, JobInfo jobInfo, String csvFileName)
        throws IOException, AsyncApiException {

        List<BatchInfo> batchInfos = new ArrayList<BatchInfo>();
        BufferedReader rdr = null;

        try {
            rdr = new BufferedReader(new InputStreamReader(new FileInputStream(csvFileName)));
        } catch (FileNotFoundException ex) {
            System.out.println(csvFileName + " file not found!");
            System.exit(1);
        }

        // read the CSV header row

        byte[] headerBytes = (rdr.readLine().replaceAll("\\|", ",") + "\n").getBytes("UTF-8");

        int headerBytesLength = headerBytes.length;
        File tmpFile = File.createTempFile("bulkAPIInsert", ".csv");

        // Split the CSV file into multiple batches
        try {
            FileOutputStream tmpOut = new FileOutputStream(tmpFile);
            int maxBytesPerBatch = 10000000; // 10 million bytes per batch
            int maxRowsPerBatch = 10000; // 10 thousand rows per batch
            int currentBytes = 0;
            int currentLines = 0;
            String nextLine;

            while ((nextLine = rdr.readLine()) != null) {
                byte[] bytes = (nextLine.replaceAll("\\|", ",") + "\n").getBytes("UTF-8");

                if (currentBytes + bytes.length > maxBytesPerBatch || currentLines > maxRowsPerBatch) {
                    createBatch(tmpOut, tmpFile, batchInfos, connection, jobInfo);
                    currentBytes = 0;
                    currentLines = 0;
                }

                if (currentBytes == 0) {
                    tmpOut = new FileOutputStream(tmpFile);
                    tmpOut.write(headerBytes);
                    currentBytes = headerBytesLength;
                    currentLines = 1;
                }

                tmpOut.write(bytes);
                currentBytes += bytes.length;
                currentLines++;
            }

            if (currentLines > 1) {
                createBatch(tmpOut, tmpFile, batchInfos, connection, jobInfo);
            }
        } finally {
            tmpFile.delete();
        }
        return batchInfos;
    }

    /**
     * Create a batch by uploading the contents of the file.
     * This closes the output stream.
     *
     * @param tmpOut The output stream used to write the CSV data for a single batch.
     * @param tmpFile The file associated with the above stream.
     * @param batchInfos The batch info for the newly created batch is added to this list.
     * @param connection The BulkConnection used to create the new batch.
     * @param jobInfo The JobInfo associated with the new batch.
     */
    private void createBatch(FileOutputStream tmpOut, File tmpFile, List<BatchInfo> batchInfos,
        BulkConnection connection, JobInfo jobInfo) throws IOException, AsyncApiException {

        tmpOut.flush();
        tmpOut.close();
        FileInputStream tmpInputStream = new FileInputStream(tmpFile);

        try {
            BatchInfo batchInfo = connection.createBatchFromStream(jobInfo, tmpInputStream);
            System.out.println(batchInfo);
            batchInfos.add(batchInfo);

        } finally {
            tmpInputStream.close();
        }
    }
}