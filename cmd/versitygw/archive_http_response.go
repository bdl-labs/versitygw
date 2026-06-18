package main

import (
	"errors"
	"net/http"

	"github.com/gofiber/fiber/v3"
)

func writeArchiveError(c fiber.Ctx, status int, message string) error {
	return c.Status(status).SendString(message)
}

func writeArchiveErrorFrom(c fiber.Ctx, err error, fallbackStatus int) error {
	if err == nil {
		return nil
	}

	var fiberErr *fiber.Error
	if errors.As(err, &fiberErr) {
		return writeArchiveError(c, fiberErr.Code, fiberErr.Message)
	}

	if fallbackStatus <= 0 {
		fallbackStatus = http.StatusInternalServerError
	}
	return writeArchiveError(c, fallbackStatus, err.Error())
}
