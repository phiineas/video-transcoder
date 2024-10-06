import { ReceiveMessageCommand, SQSClient, DeleteMessageCommand } from '@aws-sdk/client-sqs';
import { ECSClient, RunTaskCommand } from '@aws-sdk/client-ecs';
import type { S3Event } from 'aws-lambda';
import dotenv from 'dotenv';

dotenv.config();

const region = process.env.AWS_REGION || "ap-south-1";
const accessKeyId = process.env.AWSACCESSKEYID;
const secretAccessKey = process.env.AWSSECRETACCESSKEY;

if (!accessKeyId || !secretAccessKey) {
    throw new Error("AWSACCESSKEYID and AWSSECRETACCESSKEY must be defined");
}

const client = new SQSClient({
    region,
    credentials: {
        accessKeyId,
        secretAccessKey,
    },
});

const ecsClient = new ECSClient({
    region,
    credentials: {
        accessKeyId,
        secretAccessKey,
    },
});

async function init() {
    const command = new ReceiveMessageCommand({
        QueueUrl: "https://sqs.ap-south-1.amazonaws.com/992382567908/temprawfiles3queue",
        MaxNumberOfMessages: 1,
        WaitTimeSeconds: 20
    });

    while (true) {
        const { Messages } = await client.send(command);
        if (!Messages) {
            console.log("no messages");
            continue;
        }

        try {
            for (const message of Messages) {
                const { MessageId, Body } = message;
                console.log(`message received`, { MessageId, Body});
    
                if (!Body) {
                    console.log("no body");
                    continue;
                }
    
                // parse the body as S3Event
    
                const event: S3Event = JSON.parse(Body) as S3Event;

                if ("Service" in event && "Event" in event) {
                    if (event.Event === "s3:TestEvent") {
                        await client.send(new DeleteMessageCommand({
                            QueueUrl: "https://sqs.ap-south-1.amazonaws.com/992382567908/temprawfiles3queue",
                            ReceiptHandle: message.ReceiptHandle
                        }));
                        continue;
                    }
                }

                // process the event

                for (const record of event.Records) {
                    const { s3 } = record;
                    const { 
                        bucket, 
                        object: { key },
                    } = s3;

                    const runTaskCommand = new RunTaskCommand({
                        taskDefinition: "arn:aws:ecs:ap-south-1:992382567908:task-definition/video-transcoder",
                        cluster: "arn:aws:ecs:ap-south-1:992382567908:cluster/cluster2",
                        launchType: "FARGATE",
                        networkConfiguration: {
                            awsvpcConfiguration: {
                                assignPublicIp: "ENABLED",
                                securityGroups: ["sg-0acba8c2d35a6d6bb"],
                                subnets: [
                                    "subnet-0abdbee4795cc53f3",
                                    "subnet-0bbded213c149cda7",
                                    "subnet-0ce8d4f9569fa80e3",
                                ],
                            },
                        },
                        overrides: {
                            containerOverrides: [
                                { 
                                    name: "video-transcoder", 
                                    environment: [
                                        { name: 'BUCKET_NAME', value: bucket.name }, 
                                        { name: 'KEY', value: key }
                                    ],
                                },
                            ],
                        }
                    });

                    await ecsClient.send(runTaskCommand);

                    // delete the message from the queue
                    await client.send(new DeleteMessageCommand({
                        QueueUrl: "https://sqs.ap-south-1.amazonaws.com/992382567908/temprawfiles3queue",
                        ReceiptHandle: message.ReceiptHandle
                    }));
                }
            }
        } catch (err) {
            console.log(err);
        }
    }
}

init();
