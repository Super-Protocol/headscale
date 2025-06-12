package test

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var (
	// Глобальные переменные для хранения CA и его ключа
	caCert     *x509.Certificate
	caKey      *rsa.PrivateKey
	caFilePath string
	caKeyPath  string

	// Мьютекс для безопасного доступа к CA
	caMutex sync.Mutex

	// Флаг, указывающий, был ли уже сгенерирован CA
	caGenerated bool
)

// GenerateOrLoadCA генерирует CA, если он еще не был сгенерирован, или возвращает существующий
func GenerateOrLoadCA(certDir string) (string, error) {
	caMutex.Lock()
	defer caMutex.Unlock()

	if caGenerated && caFilePath != "" {
		return caFilePath, nil
	}

	// Создаем директорию, если она не существует
	if err := os.MkdirAll(certDir, 0755); err != nil {
		return "", fmt.Errorf("не удалось создать директорию для сертификатов: %w", err)
	}

	caFilePath = filepath.Join(certDir, "ca.crt")
	caKeyPath = filepath.Join(certDir, "ca.key")

	// Проверяем, существуют ли уже файлы CA
	if _, err := os.Stat(caFilePath); err == nil {
		if _, err := os.Stat(caKeyPath); err == nil {
			// Загружаем существующий CA и ключ
			caCertBytes, err := os.ReadFile(caFilePath)
			if err != nil {
				return "", fmt.Errorf("не удалось прочитать CA сертификат: %w", err)
			}

			caKeyBytes, err := os.ReadFile(caKeyPath)
			if err != nil {
				return "", fmt.Errorf("не удалось прочитать CA ключ: %w", err)
			}

			// Декодируем PEM
			caBlock, _ := pem.Decode(caCertBytes)
			if caBlock == nil {
				return "", fmt.Errorf("не удалось декодировать CA сертификат")
			}

			caKeyBlock, _ := pem.Decode(caKeyBytes)
			if caKeyBlock == nil {
				return "", fmt.Errorf("не удалось декодировать CA ключ")
			}

			// Парсим сертификат и ключ
			caCert, err = x509.ParseCertificate(caBlock.Bytes)
			if err != nil {
				return "", fmt.Errorf("не удалось распарсить CA сертификат: %w", err)
			}

			caKey, err = x509.ParsePKCS1PrivateKey(caKeyBlock.Bytes)
			if err != nil {
				return "", fmt.Errorf("не удалось распарсить CA ключ: %w", err)
			}

			caGenerated = true
			return caFilePath, nil
		}
	}

	// Генерируем новый CA и ключ
	var err error
	caKey, err = rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return "", fmt.Errorf("не удалось сгенерировать CA ключ: %w", err)
	}

	caTemplate := x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject: pkix.Name{
			CommonName: "SPNetwork Test CA",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(10, 0, 0), // 10 лет срок действия
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	caBytes, err := x509.CreateCertificate(rand.Reader, &caTemplate, &caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return "", fmt.Errorf("не удалось создать CA сертификат: %w", err)
	}

	caCert = &caTemplate

	// Сохраняем CA сертификат
	caOut, err := os.Create(caFilePath)
	if err != nil {
		return "", fmt.Errorf("не удалось создать файл CA: %w", err)
	}
	defer caOut.Close()

	err = pem.Encode(caOut, &pem.Block{Type: "CERTIFICATE", Bytes: caBytes})
	if err != nil {
		return "", fmt.Errorf("не удалось записать CA сертификат: %w", err)
	}

	// Сохраняем CA ключ
	caKeyOut, err := os.Create(caKeyPath)
	if err != nil {
		return "", fmt.Errorf("не удалось создать файл CA ключа: %w", err)
	}
	defer caKeyOut.Close()

	err = pem.Encode(caKeyOut, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(caKey)})
	if err != nil {
		return "", fmt.Errorf("не удалось записать CA ключ: %w", err)
	}

	caGenerated = true
	return caFilePath, nil
}

// GenerateCertificate генерирует сертификат, подписанный CA
func GenerateCertificate(certDir, commonName string) (certPath, keyPath string, err error) {
	caMutex.Lock()
	defer caMutex.Unlock()

	if !caGenerated {
		_, err = GenerateOrLoadCA(certDir)
		if err != nil {
			return "", "", fmt.Errorf("не удалось сгенерировать/загрузить CA: %w", err)
		}
	}

	// Генерируем ключ
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return "", "", fmt.Errorf("не удалось сгенерировать ключ: %w", err)
	}

	// Создаем шаблон сертификата
	template := x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject: pkix.Name{
			CommonName: commonName,
		},
		DNSNames:    []string{"localhost"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().AddDate(1, 0, 0), // 1 год срок действия
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}

	// Подписываем сертификат с помощью CA
	certBytes, err := x509.CreateCertificate(rand.Reader, &template, caCert, &key.PublicKey, caKey)
	if err != nil {
		return "", "", fmt.Errorf("не удалось создать сертификат: %w", err)
	}

	// Пути к файлам
	certPath = filepath.Join(certDir, commonName+".crt")
	keyPath = filepath.Join(certDir, commonName+".key")

	// Сохраняем сертификат
	certOut, err := os.Create(certPath)
	if err != nil {
		return "", "", fmt.Errorf("не удалось создать файл сертификата: %w", err)
	}
	defer certOut.Close()

	err = pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: certBytes})
	if err != nil {
		return "", "", fmt.Errorf("не удалось записать сертификат: %w", err)
	}

	// Сохраняем ключ
	keyOut, err := os.Create(keyPath)
	if err != nil {
		return "", "", fmt.Errorf("не удалось создать файл ключа: %w", err)
	}
	defer keyOut.Close()

	err = pem.Encode(keyOut, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	if err != nil {
		return "", "", fmt.Errorf("не удалось записать ключ: %w", err)
	}

	return certPath, keyPath, nil
}
