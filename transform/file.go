package transform

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/xuenqlve/common/errors"
	"go.yaml.in/yaml/v3"
)

const encryptKey = "0123456789abc-de"

func ConfigFromFile(path string) (map[string]any, error) {
	cfgData := map[string]any{}
	if strings.HasSuffix(path, ".toml") {
		_, err := toml.DecodeFile(path, &cfgData)
		if err != nil {
			return nil, errors.Trace(err)
		}
	} else if strings.HasSuffix(path, ".json") {
		content, err := os.ReadFile(path)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if err = json.Unmarshal(content, &cfgData); err != nil {
			return nil, errors.Trace(err)
		}
	} else if strings.HasSuffix(path, ".yaml") {
		context, err := os.ReadFile(path)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if err = yaml.Unmarshal(context, &cfgData); err != nil {
			return nil, errors.Trace(err)
		}
	} else {
		return nil, errors.Errorf("unrecognized path %s", path)
	}
	return cfgData, nil
}

func ConfigFromString(content string, contentType string) (map[string]any, error) {
	cfgData := map[string]any{}
	if contentType == "toml" {
		_, err := toml.Decode(content, &cfgData)
		if err != nil {
			return nil, errors.Trace(err)
		}
	} else if contentType == "json" {
		if err := json.Unmarshal([]byte(content), &cfgData); err != nil {
			return nil, errors.Trace(err)
		}
	} else if contentType == "yaml" {
		if err := yaml.Unmarshal([]byte(content), &cfgData); err != nil {
			return nil, errors.Trace(err)
		}
	} else {
		return nil, fmt.Errorf("unknown content type %s", contentType)
	}
	return cfgData, nil
}

func GetFullLogPath(path, fileName string) string {
	if HasSuffix(path, "/") {
		return path + fileName
	}
	return path + "/" + fileName
}

func HasSuffix(s, suffix string) bool {
	return len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix
}

func Cipher2Password(cipher string) (pwd string, err error) {
	if cipher == "" {
		return
	}
	data, err := base64.StdEncoding.DecodeString(cipher)
	origData, err := AesDecrypt(data, []byte(encryptKey))
	if err != nil {
		return
	}
	pwd = string(origData)
	return
}

func AesDecrypt(encrypt, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	blockSize := block.BlockSize()
	blockMode := cipher.NewCBCDecrypter(block, key[:blockSize])
	origData := make([]byte, len(encrypt))
	blockMode.CryptBlocks(origData, encrypt)
	origData = pkCS7UnPadding(origData)
	return origData, nil
}

func pkCS7UnPadding(origData []byte) []byte {
	length := len(origData)
	unPadding := int(origData[length-1])
	return origData[:(length - unPadding)]
}
