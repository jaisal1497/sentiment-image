#from email import message
#from traceback import print_tb
#from urllib import response
import boto3
import json

from keras.applications.resnet import ResNet50, preprocess_input, decode_predictions
from keras.preprocessing import image
import numpy as np

print("Loading..")
fcount=0
# Setup aws credentials
aws_access_id = ""
aws_secret_access_key = ""

# Setup queue
sqs = boto3.client("sqs", region_name='ap-south-1', aws_access_key_id=aws_access_id, 
                   aws_secret_access_key=aws_secret_access_key)
s3 = boto3.client("s3", region_name='ap-south-1', aws_access_key_id=aws_access_id, 
                   aws_secret_access_key=aws_secret_access_key)
s3_resource = boto3.resource("s3", region_name='ap-south-1', aws_access_key_id=aws_access_id, 
                   aws_secret_access_key=aws_secret_access_key)
bucket = "jaisal-image-service"
queue_url = 'https://sqs.ap-south-1.amazonaws.com/675802422046/ImageJobs.fifo'
image_queue = 'https://sqs.ap-south-1.amazonaws.com/675802422046/responseservice.fifo' #change SQS URL
# Setup model
model = ResNet50(weights='weights.h5')

def process_message(messages):
    # convert json string to object
    messages = json.loads(messages)
    image_labels=dict()
    print(messages)
    for message in messages['images']:
        print(f"processing message inside process message: {message}")
        labels = predict_resetnet(message["image_path"])
        # do what you want with the message here
        save_output(message["image_id"], labels)
        if message["image_id"] not in image_labels:
            image_labels[message["image_id"]] = labels
    send_response_to_sqs(image_labels)
    pass




def predict_resetnet(image_path):
    global fcount
    print("Loading image... %s" % image_path)
    s3_object = s3_resource.Object(bucket, image_path)
    fcount+=1
    fname="tmp"+str(fcount)+".jpg"
    img=s3_object.download_file(fname)
    img = image.load_img(fname, target_size=(224, 224))
    print(img)
    x = image.img_to_array(img)
    x = np.expand_dims(x, axis=0) 
    x = preprocess_input(x)
    preds = model.predict(x)

    labels = decode_predictions(preds, top=1)[0]
    print(labels)
    return labels


def send_response_to_sqs(image_labels):
    sqs = boto3.client("sqs", region_name='ap-south-1', aws_access_key_id=aws_access_id,
                       aws_secret_access_key=aws_secret_access_key)
    print("sending response: ", image_labels)
    sqs.send_message(
        QueueUrl=image_queue,
        MessageBody=json.dumps({
            'image_labels': str(image_labels)
        }),
        MessageGroupId="01"
    )


def save_output(image_id, labels):
    txt_data="saving output...image id "+str(image_id) +" output label "+labels[0][1]
    out_object=s3_resource.Object(bucket,str(image_id)+'.txt')
    out_object.put(Body=txt_data)
    
    # TODO: Do something with the labels


if __name__ == "__main__":
    while True:
        print("waiting for next message..")
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=5
        )

        if 'Messages' in response:
            messages = response['Messages']
            for message in messages:
                print(f"new messsage received: {message['Body']}")
                print("message body is ", message['Body'])
                process_message(message['Body'])
                sqs.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=message['ReceiptHandle']
                )
