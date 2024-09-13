# Stream-Zip-Files

## Overview

**Stream-Zip-Files** is a Scala library that enables efficient streaming and zipping of files from AWS S3 or DynamoDB, allowing users to download single or multiple files as a zip. Using the power of the `fs2` streaming library, it ensures minimal memory footprint even when handling large numbers of files or large file sizes.

With this library, you can stream the files directly from the source, compress them on the fly into a zip archive, and serve the zip to clients without loading everything into memory.

You can find a blog for this library [here](https://www.google.com)

## Features

- **Efficient streaming**: Uses `fs2` to stream files from S3 or DynamoDB, keeping memory usage low.
- **On-the-fly zipping**: Streams files directly into a zip archive, avoiding the need to store files locally before zipping.
- **Supports multiple sources**: Fetch files from both AWS S3 and DynamoDB.
- **Low memory footprint**: The entire process is designed to be lightweight and scalable, perfect for handling large downloads.

## Getting Started

### Prerequisites

- Scala 2.13+
- sbt 1.4+
- AWS SDK (S3, DynamoDB)

## Usage

This library provides a simple, scalable way to stream files from S3 or DynamoDB and zip them into a downloadable archive. Here's a breakdown of how it works:

1) Download Files from S3
Using the AWS S3 SDK, you can stream files directly from an S3 bucket.

2) Download Files from DynamoDB
You can retrieve files stored in DynamoDB as streams.

3) Zip Files On-The-Fly
As files are streamed, they are simultaneously zipped into a single archive that can be served to clients.
