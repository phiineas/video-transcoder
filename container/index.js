const { S3Client, GetObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');
const fs = require('node:fs/promises');
const fs2 = require('node:fs');
const path = require('node:path');
const ffmpeg = require('fluent-ffmpeg');

const RESOLUTIONS = [
    { name: "360p", width: 640, height: 360 },
    { name: "480p", width: 854, height: 480 },
    { name: "720p", width: 1280, height: 720 },
    { name: "1080p", width: 1920, height: 1080 },
];

const region = process.env.AWS_REGION || "ap-south-1";
const accessKeyId = process.env.AWSACCESSKEYID;
const secretAccessKey = process.env.AWSSECRETACCESSKEY;

if (!accessKeyId || !secretAccessKey) {
    throw new Error("AWSACCESSKEYID and AWSSECRETACCESSKEY must be defined");
}

const s3Client = new S3Client({
    region,
    credentials: {
        accessKeyId,
        secretAccessKey,
    },
});

const BUCKET_NAME = process.env.BUCKET_NAME;
const KEY = process.env.KEY;

async function init() {
    const command = new GetObjectCommand({
        Bucket: BUCKET_NAME,
        Key: KEY,
    });

    const result = await s3Client.send(command);

    const originalFilePath = `original-video.mp4`;

    await fs.writeFile(originalFilePath, result.Body);

    const originalVideoPath = path.resolve(originalFilePath);

    // transcode the video

    const promises = RESOLUTIONS.map((resolution) => {
        const output = `video-${resolution.name}.mp4`;

        return new Promise((resolve) => {
            ffmpeg(originalVideoPath)
            .output(output)
            .withVideoCodec('libx264')
            .withAudioCodec('aac')
            .withSize(`${resolution.width}x${resolution.height}`)
            .on('start', () => {
                console.log(`transcoding video to ${resolution.name}`);
            })
            .on('end', async () => {
                const putCommand = new PutObjectCommand({
                    Bucket: "production.phiineas.xyz",
                    Key: output,
                    Body: fs2.createReadStream(path.resolve(output)),
                });
                await s3Client.send(putCommand);
                console.log(`uploaded ${output}`);
                resolve();
            })
            .format('mp4')
            .run();
        });
    });

    await Promise.all(promises);
}

init();
