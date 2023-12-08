package com.google.cloud.spanner.pgadapter.sample.repository;

import com.google.cloud.spanner.pgadapter.sample.model.LobTest;
import org.springframework.data.jpa.repository.JpaRepository;

public interface LobTestRepository extends JpaRepository<LobTest, String> {

}
