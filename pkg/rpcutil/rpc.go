/*
http://www.apache.org/licenses/LICENSE-2.0.txt


Copyright 2015 Intel Corporation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package rpcutil

import (
	"fmt"
	"time"
	"io/ioutil"
	"errors"

	log "github.com/Sirupsen/logrus"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"crypto/tls"
	"crypto/x509"

)

// GetClientConnection returns a grcp.ClientConn that is unsecured
// TODO: Add TLS security to connection
//func GetClientConnection(addr string, port int) (*grpc.ClientConn, error) {
//	log.WithFields(log.Fields{
//		"_module":     "pkg/rpcutil/rpc.go",
//		"_block":      "GetClientConnectio",
//		"addr": addr,
//		"port": port,
//	}).Info("Debug Iza - getting client connection")
//
//	grpcDialOpts := []grpc.DialOption{
//		grpc.WithTimeout(2 * time.Second),
//	}
//	grpcDialOpts = append(grpcDialOpts, grpc.WithInsecure())
//	conn, err := grpc.Dial(fmt.Sprintf("%v:%v", addr, port), grpcDialOpts...)
//	if err != nil {
//		return nil, err
//	}
//	return conn, nil
//}


func GetClientConnection(addr string, port int) (*grpc.ClientConn, error) {
	log.WithFields(log.Fields{
		"_module":     "pkg/rpcutil/rpc.go",
		"_block":      "GetClientConnectio",
		"addr": addr,
		"port": port,
	}).Info("Debug Iza - getting client connection")

	certPath := "/home/iza/Projects/src/github.com/square/certstrap/bin/out/Alice.crt"
	keyPath := "/home/iza/Projects/src/github.com/square/certstrap/bin/out/Alice.key"

	certificate, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		log.WithFields(log.Fields{
			"_module":     "pkg/rpcutil/rpc.go",
			"_block":      "GetClientConnectio",
			"addr": addr,
			"port": port,
			"certPath": certPath,
			"keyPath": keyPath,
		}).Errorf("Cannot load grpc key pair, err = %s", err.Error())
		return nil, err
	}

	certPool := x509.NewCertPool()
	bs, err := ioutil.ReadFile("/home/iza/Projects/src/github.com/square/certstrap/bin/out/My_Root_CA.crt")
	if err != nil {
		log.WithFields(log.Fields{
			"_module":     "pkg/rpcutil/rpc.go",
			"_block":      "GetClientConnectio",
			"addr": addr,
			"port": port,
		}).Errorf("Cannot read My_Root_CA, err = %s", err.Error())
		return nil, err
	}

	if ok := certPool.AppendCertsFromPEM(bs); !ok {
		return nil, errors.New("Cannot append cert to pool")
	}

	serverName := fmt.Sprintf("%v:%v", addr, port)
	//serverName := fmt.Sprintf("%v", addr)

	log.WithFields(log.Fields{
		"_module":     "pkg/rpcutil/rpc.go",
		"_block":      "GetClientConnection",
		"addr": addr,
		"port": port,
		"server_name": serverName,
	}).Info("creating a server name")


	credential := credentials.NewTLS(&tls.Config{
		ServerName: fmt.Sprintf("%v", addr),

		Certificates: []tls.Certificate{certificate},
		RootCAs: certPool,
	})

	grpcDialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(credential),
		grpc.WithTimeout(2 * time.Second),
	}

	//grpcDialOpts := []grpc.DialOption{
	//	grpc.WithTimeout(2 * time.Second),
	//}
	// grpcDialOpts = append(grpcDialOpts, grpc.WithInsecure())
	conn, err := grpc.Dial(serverName, grpcDialOpts...)
	if err != nil {
		log.WithFields(log.Fields{
			"_module":     "pkg/rpcutil/rpc.go",
			"_block":      "GetClientConnectio",
			"addr": addr,
			"port": port,
			"err": err,
		}).Info("Debug Iza - cannot get client connection")
		return nil, err
	}

	log.WithFields(log.Fields{
		"_module":     "pkg/rpcutil/rpc.go",
		"_block":      "GetClientConnectio",
		"addr": addr,
		"port": port,
	}).Info("Debug Iza - SUCCESS in get client connection")
	return conn, nil
}