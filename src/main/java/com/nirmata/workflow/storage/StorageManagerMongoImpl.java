/**
 * Copyright 2014 Nirmata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nirmata.workflow.storage;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.mongodb.client.model.Filters.eq;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.TaskId;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageManagerMongoImpl implements StorageManager {
    private static final String KAFKA_WORKFLOW_DBNAME = "kafkaworkflow";
    private static final String RUN_COLL = "runinfo";

    private static final String FLD_ID = "_id";
    private static final String FLD_TASKS = "tasks";
    private static final String FLD_MODIFIED_TS = "modified";
    private static final String FLD_RUNNABLE = "runnable";
    private static final String FLD_STARTED_TASK = "started";
    private static final String FLD_TASK_EXEC_RESULT = "execResult";

    private static final String DOT_REPLACE = "_dotInId_";

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final MongoClient client;
    private final MongoCollection<Document> collection;

    public StorageManagerMongoImpl(String uri, String namespace, String version) {
        try {
            this.client = MongoClients.create(
                    MongoClientSettings.builder().applyConnectionString(new ConnectionString(uri))
                            .applicationName(KAFKA_WORKFLOW_DBNAME + "_" + namespace + "_" + version)
                            .build());
            this.collection = this.client.getDatabase(KAFKA_WORKFLOW_DBNAME)
                    .getCollection(RUN_COLL + "_" + namespace + "_" + version);
        } catch (Exception e) {
            log.error("Error connecting to Mongo with {}", uri, e);
            throw e;
        }
    }

    @Override
    public void createRun(RunId runId, byte[] runnableBytes) {
        try {
            collection.insertOne(new Document()
                    .append(FLD_ID, runId.getId())
                    .append(FLD_MODIFIED_TS, new Date())
                    .append(FLD_RUNNABLE, toStr(runnableBytes)));
        } catch (Exception e) {
            log.warn("Could not create new run with id {} ignoring, mostly duplicate", runId, e);
        }
    }

    @Override
    public void updateRun(RunId runId, byte[] runnableBytes) {
        setField(runId, FLD_RUNNABLE, runnableBytes);
    }

    private String toStr(byte[] bytes) {
        // String from bytes should be ok considering that bytes are actually
        // created by serializing our java objects to JSON. So invalid UTF chars
        // should not be a concern
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private byte[] toBytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] getRunnable(RunId runId) {
        Bson projectionFields = Projections.fields(
                Projections.include(FLD_RUNNABLE));
        Document doc = collection.find(eq(FLD_ID, runId.getId()))
                .projection(projectionFields)
                .first();
        if (doc == null) {
            return null;
        }

        return toBytes(doc.getString(FLD_RUNNABLE));
    }

    @Override
    public List<String> getRunIds() {
        List<String> runIds = new LinkedList<String>();
        Bson projectionFields = Projections.fields(
                Projections.include(FLD_ID));
        collection.find()
                .projection(projectionFields).forEach(curr -> runIds.add(curr.getString(FLD_ID)));
        return runIds;
    }

    @Override
    public Map<String, byte[]> getRuns() {
        Map<String, byte[]> runInfos = new HashMap<String, byte[]>();
        Bson projectionFields = Projections.fields(Projections.include(FLD_RUNNABLE));
        collection.find().projection(projectionFields)
                .forEach(curr -> runInfos.put(curr.getString(FLD_ID), toBytes(curr.getString(FLD_RUNNABLE))));
        return runInfos;
    }

    @Override
    public byte[] getStartedTask(RunId runId, TaskId taskId) {
        return getTaskField(runId, taskId, FLD_STARTED_TASK);
    }

    @Override
    public void setStartedTask(RunId runId, TaskId taskId, byte[] startedTaskData) {
        setTaskField(runId, taskId, FLD_STARTED_TASK, startedTaskData);
    }

    @Override
    public RunRecord getRunDetails(RunId runId) {
        Bson projectionFields = Projections.fields(
                Projections.include(FLD_RUNNABLE),
                Projections.include(FLD_TASKS));
        Document doc = collection.find(eq(FLD_ID, runId.getId()))
                .projection(projectionFields)
                .first();
        if (doc == null) {
            return null;
        }

        byte[] runnable = toBytes(doc.getString(FLD_RUNNABLE));

        Map<String, byte[]> startedTasks = new HashMap<String, byte[]>();
        Map<String, byte[]> completedTasks = new HashMap<String, byte[]>();

        try {
            Document allTasks = doc.get(FLD_TASKS, Document.class);
            if (allTasks != null) {
                for (String taskId : allTasks.keySet()) {
                    Document task = allTasks.get(taskId, Document.class);
                    if (task.getString(FLD_STARTED_TASK) != null) {
                        startedTasks.put(decodeDot(taskId),
                                toBytes(task.getString(FLD_STARTED_TASK)));
                    }
                    if (task.getString(FLD_TASK_EXEC_RESULT) != null) {
                        completedTasks.put(decodeDot(taskId),
                                toBytes(task.getString(FLD_TASK_EXEC_RESULT)));
                    }
                }
            }
        } catch (Exception ex) {
            log.error("Could not retrieve tasks for runId {}", runId, ex);
        }

        return new RunRecord(runnable, startedTasks, completedTasks);
    }

    @Override
    public byte[] getTaskExecutionResult(RunId runId, TaskId taskId) {
        return getTaskField(runId, taskId, FLD_TASK_EXEC_RESULT);
    }

    @Override
    public void saveTaskResult(RunId runId, TaskId taskId, byte[] taskResultData) {
        setTaskField(runId, taskId, FLD_TASK_EXEC_RESULT, taskResultData);

    }

    @Override
    public boolean clean(RunId runId) {
        Bson query = eq(FLD_ID, runId.getId());
        try {
            DeleteResult result = collection.deleteOne(query);
            if (result.getDeletedCount() == 0) {
                return false;
            }
        } catch (MongoException me) {
            log.error("Unable to delete from mongo: ", me);
        }
        return true;
    }

    private byte[] getTaskField(RunId runId, TaskId taskId, String fldName) {
        Bson projectionFields = Projections.fields(
                Projections.include(FLD_TASKS));
        Document doc = collection.find(eq(FLD_ID, runId.getId()))
                .projection(projectionFields)
                .first();
        if (doc != null && doc.get(FLD_TASKS, Document.class) != null) {
            Document task = doc.get(FLD_TASKS, Document.class).get(encodeDot(taskId.getId()), Document.class);
            if (task != null && task.getString(fldName) != null) {
                return toBytes(task.getString(fldName));
            }
        }
        return null;
    }

    private void setTaskField(RunId runId, TaskId taskId, String fldName, byte[] data) {
        setField(runId, tskfld(taskId.getId(), fldName), data);
    }

    private String tskfld(String taskId, String fldName) {
        return FLD_TASKS + "." + encodeDot(taskId) + "." + fldName;
    }

    private String encodeDot(String dotField) {
        return dotField.replaceAll("\\.", DOT_REPLACE);
    }

    private String decodeDot(String dotField) {
        return dotField.replaceAll(DOT_REPLACE, ".");
    }

    private void setField(RunId runId, String fldName, byte[] data) {
        Document query = new Document().append(FLD_ID, runId.getId());
        Bson update = Updates.combine(Updates.set(fldName, toStr(data)), Updates.currentDate(FLD_MODIFIED_TS));
        try {
            UpdateResult result = collection.updateOne(query, update);
            if (result.getModifiedCount() == 0) {
                log.error("Unable to update field {}:{}", runId, fldName);
            }
        } catch (MongoException me) {
            log.error("Unable to update field {}:{}", runId, fldName, me);
        }
    }

}
