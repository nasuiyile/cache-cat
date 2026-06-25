import redis
import ssl
r = redis.Redis(
    host="localhost",
    port=6479,
    ssl=True,

    # ssl_ca_certs="C:/tmp/ca/ca.crt",      # CA
    # ssl_certfile="C:/tmp/ca/client.crt",  # 客户端证书
    # ssl_keyfile="C:/tmp/ca/client.key",   # 客户端私钥
    ssl_cert_reqs=ssl.CERT_NONE,
)

print(r.ping())