import json
import socket
from tweepy import Stream, StreamListener
from tweepy import OAuthHandler


class TweetListener(object):

    class TweetListener(StreamListener):
        def __init__(self, socket):
            print("Listener initialized")
            self.client_socket = socket

        def on_data(self, data):
            try:
                jsonmessage = json.loads(data)
                message = jsonmessage["text"].encode("utf-8")
                print(str(message))
                self.client_socket.send(message)
            except BaseException as e:
                print("Error on_data %s" % str(e))
            return True

        def on_error(self, status):
            print(status)
            return False


def twitter(connection, tracks):
    auth = OAuthHandler("P5rwBhOL39Elrk1ayT4IduXKO", "Nv41Bivx4dljOu0dRUhHadua3NCVJV503Bi79pRZAgefPD92bO")
    auth.set_access_token("1173013963199733760-vEOSnP17ebk0aTtxD7M0BBZap5JxqK",
                          "qACOxdyRrHnh5u9abi6LC9NiwcQUZ9ODFrBRcJQpjJv0G")
    tweeter_stream = Stream(auth, TweetListener(connection))
    tweeter_stream.filter(track=tracks, languages=["en"])


if __name__ == "__main__":
    # Set up host and port
    host = "localhost"
    port = 9999

    # Extract tweets with hashtag
    tracks ="corona virus COVID19 SARSCOV2"

    # Set up socket
    s = socket.socket()
    s.bind((host, port))
    print("Listening on port: %s" % str(port))

    # Listen to socket
    s.listen(5)
    connection, client_address = s.accept()

    # Connect to Twitter
    twitter(connection, tracks)


