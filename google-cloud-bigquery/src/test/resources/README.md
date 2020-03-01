Use openssl in order to generate private and public keys

```
openssl genrsa -out mykey.pem 1024
openssl pkcs8 -topk8 -nocrypt -in mykey.pem -out myrsakey_pcks8
openssl rsa -in mykey.pem -pubout > mykey.pub
```