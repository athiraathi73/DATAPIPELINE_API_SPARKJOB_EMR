package aws.datapipeline;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.datapipeline.DataPipelineClient;
import com.amazonaws.services.datapipeline.model.*;

import java.util.ArrayList;
import java.util.List;

/**
* When making video explain about the space issue
*/
public class DataPipeLineEmr {

        public static void main(String[] args) {

            AWSCredentials credentials = new BasicAWSCredentials("", "");
            DataPipelineClient dp = new DataPipelineClient(credentials);
            CreatePipelineRequest createPipeline = new CreatePipelineRequest().withName("Datapipeline-EMR-Demo-Post-Fix").withUniqueId("unique");
            CreatePipelineResult createPipelineResult = dp.createPipeline(createPipeline);
            String pipelineId = createPipelineResult.getPipelineId();

            PipelineObject emrCluster = new PipelineObject()
                    .withName("EmrClusterObj")
                    .withId("EmrClusterObj")
                    .withFields(
                            new Field().withKey("releaseLabel").withStringValue("emr-5.16.0"),
                            new Field().withKey("coreInstanceCount").withStringValue("2"), // Mentions Core (Slave) Count
                            new Field().withKey("applications").withStringValue("spark"),
                            new Field().withKey("type").withStringValue("EmrCluster"),
                            new Field().withKey("masterInstanceType").withStringValue("m3.xlarge"),
                            new Field().withKey("coreInstanceType").withStringValue("m3.xlarge")
                    );

            PipelineObject emrActivity = new PipelineObject()
                    .withName("EmrActivityObj")
                    .withId("EmrActivityObj")
                    .withFields(
                            new Field().withKey("step").withStringValue("command-runner.jar,spark-submit,--deploy-mode,cluster,--master,yarn,--executor-cores,2,--class,aws.datapipeline.DemoAWSDataPipeline,--executor-memory,5G,--driver-memory,5G,s3://aws-data-pipelines/tasks/jars/spark-notebooks-1.0-SNAPSHOT.jar,s3://aws-data-pipelines/output-via-data-pipeline"),
                            new Field().withKey("runsOn").withRefValue("EmrClusterObj"),
                            new Field().withKey("type").withStringValue("EmrActivity")
                    );

            PipelineObject schedule = new PipelineObject()
                    .withName("Every 15 Minutes")
                    .withId("DefaultSchedule")
                    .withFields(
                            new Field().withKey("type").withStringValue("Schedule"),
                            new Field().withKey("period").withStringValue("2 Years"),
                            new Field().withKey("startAt").withStringValue("FIRST_ACTIVATION_DATE_TIME")
                    );

            PipelineObject defaultObject = new PipelineObject()
                    .withName("Default")
                    .withId("Default")
                    .withFields(
                            new Field().withKey("failureAndRerunMode").withStringValue("CASCADE"),
                            new Field().withKey("schedule").withRefValue("DefaultSchedule"),
                            new Field().withKey("resourceRole").withStringValue("DataPipelineDefaultResourceRole"),
                            new Field().withKey("role").withStringValue("DataPipelineDefaultRole"),
                            new Field().withKey("pipelineLogUri").withStringValue("s3://aws-data-pipelines/tasks/logs"),
                            new Field().withKey("scheduleType").withStringValue("cron")
                    );

            List<PipelineObject> pipelineObjects = new ArrayList<>();

            pipelineObjects.add(emrActivity);
            pipelineObjects.add(emrCluster);
            pipelineObjects.add(defaultObject);
            pipelineObjects.add(schedule);

            PutPipelineDefinitionRequest putPipelineDefintion = new PutPipelineDefinitionRequest()
                    .withPipelineId(pipelineId)
                    .withPipelineObjects(pipelineObjects);

            PutPipelineDefinitionResult putPipelineResult = dp.putPipelineDefinition(putPipelineDefintion);
            System.out.println(putPipelineResult);

            ActivatePipelineRequest activatePipelineReq = new ActivatePipelineRequest()
                    .withPipelineId(pipelineId);
            ActivatePipelineResult activatePipelineRes = dp.activatePipeline(activatePipelineReq);

            System.out.println(activatePipelineRes);
            System.out.println(pipelineId);

        }

    }


