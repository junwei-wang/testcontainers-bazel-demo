package products

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	_ "github.com/trinodb/trino-go-client/trino"
)

func TestTrino(t *testing.T) {
	ctx := context.Background()

	image := "registry.ddbuild.io/apm-trino:latest" // TODO: latest image in production
	// TODO: to optimize the CPU and memory for performance
	trinoContainer, err := Run(
		ctx,
		image,
		WithCmd("/usr/lib/trino/bin/run-trino", "-Dcatalog.management=dynamic"),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := trinoContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate trinoContainer: %s", err)
		}
	})

	connStr, err := trinoContainer.ConnectionString(ctx, "sslmode=disable")
	assert.NoError(t, err)

	time.Sleep(5 * time.Second)

	fmt.Println("@JW: " + connStr)

	// connStr1 := "http://localhost:8080"
	db, err := sql.Open("trino", connStr) // TODO: replace the client
	assert.NoError(t, err)

	// TODO: before creating the catalog, make sure the container is properly until fully up
	createEvpCatalogSQL := `CREATE CATALOG eventplatform USING eventplatform WITH (
		"eventplatform.event_store_target" = 'event-store-api-grpc.us1.staging.dog:443', 
		"eventplatform.is_local_mode" = 'true', 
		"eventplatform.event_store_api_target_release_key" = 'logs-event-store-api-default', 
		"eventplatform.reader_target" = 'event-store-reader-grpc.us1.staging.dog:443', 
		"eventplatform.services_permitted_to_bypass_rbac" = 'my-service,other-service', 
		"aaa.zoltron_target" = 'mocked'
	)`
	_, err = db.Query(createEvpCatalogSQL)
	assert.NoError(t, err)

	catalogs, err := db.Query("SHOW CATALOGS")
	assert.NoError(t, err)

	defer catalogs.Close()
	var catalogNames []string
	for catalogs.Next() {
		var name string
		if err := catalogs.Scan(&name); err != nil {
			log.Fatal(err)
			assert.NoError(t, err)
		}
		catalogNames = append(catalogNames, name)
		// fmt.Printf("Having %s \n", name)
	}
	assert.Contains(t, catalogNames, "eventplatform")

	// time.Sleep(1000 * time.Second)
}
