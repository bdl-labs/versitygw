package main

import (
	"net/http"
	"strings"

	"github.com/gofiber/fiber/v3"
	"github.com/versity/versitygw/s3api"
	"github.com/versity/versitygw/s3api/middlewares"
)

func archiveRoute(method, path string, handlers ...fiber.Handler) []s3api.Option {
	wrapped := append([]fiber.Handler{archiveRouteCORSHandler()}, handlers...)
	options := []s3api.Option{
		s3api.WithRoute(method, path, wrapped...),
	}

	if strings.EqualFold(method, http.MethodOptions) {
		return options
	}

	options = append(options, s3api.WithRoute(http.MethodOptions, path, archiveRoutePreflightHandler(), archiveRouteCORSHandler()))
	return options
}

func archiveRouteCORSHandler() fiber.Handler {
	return func(c fiber.Ctx) error {
		if err := middlewares.ApplyDefaultCORS(corsAllowOrigin)(c); err != nil {
			return err
		}
		return c.Next()
	}
}

func archiveRoutePreflightHandler() fiber.Handler {
	return func(c fiber.Ctx) error {
		if err := middlewares.ApplyDefaultCORSPreflight(corsAllowOrigin)(c); err != nil {
			return err
		}
		return middlewares.ApplyDefaultCORS(corsAllowOrigin)(c)
	}
}
