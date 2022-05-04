
import json
import boto3
import nltk
from textblob import TextBlob
import json
from urllib import request
nltk.data.path.append(".")
nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')
print("Loading..")

# Setup aws credentials
aws_access_id = ""
aws_secret_access_key = ""

# Setup queue
sqs = boto3.client("sqs", region_name='ap-south-1', aws_access_key_id=aws_access_id, 
                   aws_secret_access_key=aws_secret_access_key)

queue_url = 'https://sqs.ap-south-1.amazonaws.com/675802422046/textservice.fifo' #change SQS URL
image_queue = 'https://sqs.ap-south-1.amazonaws.com/675802422046/responseservice.fifo' #change SQS URL
bucket = "text-service-bucketstore" #change bucket
output_file = "predictions.txt"


def process_message(message):
    # convert json string to object
    sentences = json.loads(message)
    aspect_sentiments = []
    print(f"processing message: {message}")
    for sentence in sentences:
        print(sentence)
        aspect_sentiment = analyse_sentence(sentence)
        aspect_sentiments.append(aspect_sentiment)

    send_response_to_sqs(aspect_sentiments)


def send_response_to_sqs(aspect_sentiments):
    sqs = boto3.client("sqs", region_name='ap-south-1', aws_access_key_id=aws_access_id,
                       aws_secret_access_key=aws_secret_access_key)
    print("sending response: ", aspect_sentiments)
    sqs.send_message(
        QueueUrl=image_queue,
        MessageBody=json.dumps({
            'aspect_sentiments': aspect_sentiments
        }),
        MessageGroupId="01"
    )


def upload_to_s3(file):
    s3 = boto3.client("s3", region_name='ap-south-1', aws_access_key_id=aws_access_id,
                      aws_secret_access_key=aws_secret_access_key)
    file_name = file
    s3.upload_file(file_name, bucket, file_name)


def save_output(predictions):
    print("saving output... ", predictions)
    file = open(output_file, "a")
    file.writelines(str(predictions) + '\n')
    file.close()

def analyse_sentence(sentence):
    blob = TextBlob(sentence)
    sentiment = blob.sentiment.polarity
    tag = blob.tags
    subject, opinion = find_noun_adjective(tag)
    return [subject, opinion, sentiment]


def find_noun_adjective(tags):
    noun_tags = ['NN', 'NNS', 'NNP', 'NNPS']
    adjective_tags = ['JJ', 'JJR', 'JJS']
    subject, adjective = "", ""
    for tag in tags:
        if tag[1] in noun_tags:
            subject = tag[0]
        if tag[1] in adjective_tags:
            adjective = tag[0]

    return subject, adjective

# listen to queue
if __name__ == "__main__":
    while True:
        print("waiting for next message..")
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=5,
            WaitTimeSeconds=5
        )

        if 'Messages' in response:
            message = response['Messages'][0]
            print(f"new messsage received: {message['Body']}")
            process_message(message['Body'])
            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=message['ReceiptHandle']
            )
