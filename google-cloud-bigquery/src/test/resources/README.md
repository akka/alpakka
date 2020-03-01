Use openssl in order to generate private and public keys

```
openssl genrsa -out private.pem 1024
openssl rsa -in private.pem -text
openssl pkcs8 -topk8 -nocrypt -in private.pem -out myrsakey_pcks8
openssl rsa -in private.pem -pubout > key.pub
```