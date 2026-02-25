import socket
import threading
import time
import sys
import heapq

# ------------------------
# Argument Handling
# ------------------------
if len(sys.argv) != 2:
    print("Usage: python process.py <process_id>")
    sys.exit(1)

process_id = int(sys.argv[1])
port = 5000 + process_id

# Proper intersecting quorum
quorum = {
    0: [0, 1, 2],
    1: [1, 2, 3],
    2: [2, 3, 0],
    3: [3, 0, 1]
}

lamport_clock = 0
voted_for = None
request_queue = []
replies_received = set()

lock = threading.Lock()
condition = threading.Condition(lock)

# ------------------------
# Networking
# ------------------------
def send_message(pid, message):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(("127.0.0.1", 5000 + pid))
        s.send(message.encode())
        s.close()
    except:
        pass


# ------------------------
# Server
# ------------------------
def server():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", port))
    s.listen()

    while True:
        conn, addr = s.accept()
        message = conn.recv(1024).decode()
        handle_message(message)
        conn.close()


# ------------------------
# Message Handler
# ------------------------
def handle_message(message):
    global lamport_clock, voted_for

    parts = message.split(":")
    msg_type = parts[0]

    with condition:

        if msg_type == "REQUEST":

            sender = int(parts[1])
            timestamp = int(parts[2])

            lamport_clock = max(lamport_clock, timestamp) + 1

            heapq.heappush(request_queue, (timestamp, sender))

            if voted_for is None:
                grant_vote()


        elif msg_type == "REPLY":

            sender = int(parts[1])

            replies_received.add(sender)

            condition.notify_all()


        elif msg_type == "RELEASE":

            sender = int(parts[1])

            remove_request(sender)

            if voted_for == sender:
                voted_for = None
                grant_vote()


# ------------------------
# Voting
# ------------------------
def grant_vote():
    global voted_for

    if request_queue:

        timestamp, pid = heapq.heappop(request_queue)

        voted_for = pid

        send_message(pid, f"REPLY:{process_id}")


def remove_request(pid):

    global request_queue

    request_queue = [(t, p) for (t, p) in request_queue if p != pid]

    heapq.heapify(request_queue)


# ------------------------
# Critical Section
# ------------------------
def request_cs():

    global lamport_clock, replies_received

    with condition:

        lamport_clock += 1

        timestamp = lamport_clock

        heapq.heappush(request_queue, (timestamp, process_id))

        replies_received = set()

        print(f"Process {process_id} requesting CS")

        for pid in quorum[process_id]:

            if pid != process_id:
                send_message(pid, f"REQUEST:{process_id}:{timestamp}")


    # Wait until all quorum members reply
    with condition:

        while len(replies_received) < len(quorum[process_id]) - 1:

            condition.wait()

        print(f"Process {process_id} ENTERED CS")


    time.sleep(5)


    print(f"Process {process_id} EXITING CS")


    # Remove own request
    with condition:

        remove_request(process_id)

        for pid in quorum[process_id]:

            if pid != process_id:
                send_message(pid, f"RELEASE:{process_id}")


# ------------------------
# Main
# ------------------------
if __name__ == "__main__":

    threading.Thread(target=server, daemon=True).start()

    print(f"Process {process_id} started.")

    input("Press ENTER to request CS...")

    request_cs()

    while True:
        time.sleep(1)