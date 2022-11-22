package csevent

import (
	"bytes"
	"context"
	"net/http"
	"time"

	"github.com/spf13/viper"
)

func post(path string, data []byte) (*http.Response, error) {
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	client := &http.Client{}
	request, reqError := http.NewRequest("POST", viper.GetString("computestacks.host")+path, bytes.NewBuffer(data))
	if reqError != nil {
		//sentry.CaptureException(reqError) <-- Handled downstream
		return nil, reqError
	}
	request.WithContext(ctx)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json; api_version=60")
	return client.Do(request)
}

func patch(path string, data []byte) (*http.Response, error) {
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	client := &http.Client{}
	request, reqError := http.NewRequest("PATCH", viper.GetString("computestacks.host")+path, bytes.NewBuffer(data))
	if reqError != nil {
		//sentry.CaptureException(reqError) <-- Handled downstream
		return nil, reqError
	}
	request.WithContext(ctx)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json; api_version=60")
	return client.Do(request)
}
