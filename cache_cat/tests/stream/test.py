import time
import threading
import redis

r = redis.Redis(
    host="127.0.0.1",
    port=6379,
    decode_responses=True,
)

stream = "test_stream"


def producer():
    time.sleep(3)
    msg_id = r.xadd(stream, {"msg": "hello"})
    print(f"[producer] 写入消息: {msg_id}")


threading.Thread(target=producer).start()

print("[consumer] 开始阻塞读取...")

start = time.time()

result = r.xread(
    streams={stream: "$"},
    block=10000,  # 最多阻塞 10 秒，单位毫秒
    count=1,
)

print(f"[consumer] 阻塞了 {time.time() - start:.2f} 秒")
print("[consumer] 收到:", result)