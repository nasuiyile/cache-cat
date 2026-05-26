from time import sleep
import redis
import time
import threading

r = redis.Redis(
    db=0,
    host='localhost',
    port=6379,
    decode_responses=True
)

def subscriber():
    """
    测试：
    1. 动态 subscribe 新频道
    2. pubsub ping
    3. unsubscribe
    4. close 退出
    """

    pubsub = r.pubsub()

    # 初始订阅
    pubsub.subscribe('news', 'sports')

    print("订阅者启动")
    print("初始频道: news, sports\n")

    news_count = 0
    sports_count = 0

    for message in pubsub.listen():
        msg_type = message['type']
        print(msg_type)
        # 普通消息
        if msg_type == 'message':
            channel = message['channel']
            data = message['data']

            print(f"[MESSAGE] {channel}: {data}")

            # 收到3条 news 后动态订阅 tech
            if channel == 'news':
                news_count += 1

                if news_count == 3:
                    print("\n>>> 动态订阅 tech 频道 <<<\n")
                    pubsub.subscribe('tech')

            # 收到5条 sports 后发送 ping
            if channel == 'sports':
                sports_count += 1

                if sports_count == 5:
                    print("\n>>> 发送 PUBSUB PING <<<\n")
                    print(pubsub.ping("hello-ping"))

            # 收到3条 tech 后取消订阅 news
            if channel == 'tech':
                if data.endswith("3"):
                    print("\n>>> 取消订阅 news <<<\n")
                    pubsub.unsubscribe('news')

                # tech 收到5条后退出
                if data.endswith("5"):
                    print("\n>>> 关闭 pubsub <<<\n")
                    pubsub.close()
                    break

        # subscribe 回执
        elif msg_type == 'subscribe':
            print(f"[SUBSCRIBE] channel={message['channel']} "
                  f"当前订阅数={message['data']}")

        # unsubscribe 回执
        elif msg_type == 'unsubscribe':
            print(f"[UNSUBSCRIBE] channel={message['channel']} "
                  f"剩余订阅数={message['data']}")

        # ping 回执
        elif msg_type == 'pong':
            print(f"[PONG] {message['data']}")

        else:
            print(f"[OTHER] {message}")

    print("subscriber 线程退出")


# 启动订阅线程
sub_thread = threading.Thread(target=subscriber, daemon=True)
sub_thread.start()

time.sleep(1)

# 发布消息
for i in range(10):

    r.publish('news', f"新闻消息{i+1}")
    r.publish('sports', f"体育消息{i+1}")
    r.publish('tech', f"技术消息{i+1}")

    time.sleep(1)

sleep(3)

print("\n主线程结束")