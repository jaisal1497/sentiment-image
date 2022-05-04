#from email import message
#from traceback import print_tb
#from urllib import response
import boto3
import json

from keras.applications.resnet import ResNet50, preprocess_input, decode_predictions
from keras.preprocessing import image
import numpy as np

print("Loading..")

# Setup aws credentials
aws_access_id = "access id here"
aws_secret_access_key = "secret key here"

# Setup queue
sqs = boto3.client("sqs", region_name='ap-south-1', aws_access_key_id=aws_access_id, 
                   aws_secret_access_key=aws_secret_access_key)
queue_url = 'queue url here'

# Setup model
model = ResNet50(weights='weights.h5')

def process_message(message):
    # convert json string to object
    message = json.loads(message)

    print(f"processing message: {message}")
    labels = predict_resetnet(message["image_path"])
    
    # do what you want with the message here
    save_output(message["image_id"], labels)
    pass

def predict_resetnet(image_path):
    print("Loading image... %s" % image_path)
    img = image.load_img(image_path, target_size=(224, 224))
    x = image.img_to_array(img)
    x = np.expand_dims(x, axis=0) 
    x = preprocess_input(x)
    preds = model.predict(x)

    labels = decode_predictions(preds, top=3)[0]
    print(labels)
    return labels

def save_output(image_id, labels):
    print("saving output... %s" % image_id)
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
            message = response['Messages'][0]
            print(f"new messsage received: {message['Body']}")
            process_message(message['Body'])
            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=message['ReceiptHandle']
            )
