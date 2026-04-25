import org.gradle.api.tasks.bundling.AbstractArchiveTask

plugins {
    id("com.android.library") version "8.7.0"
    kotlin("android") version "2.0.20"
}

android {
    namespace = "dev.dioxus.wavesync"
    compileSdk = 34

    defaultConfig {
        minSdk = 24
        targetSdk = 34
        consumerProguardFiles("consumer-rules.pro")
    }

    buildTypes {
        getByName("release") {
            isMinifyEnabled = false
        }
        getByName("debug") {
            isMinifyEnabled = false
        }
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }

    kotlinOptions {
        jvmTarget = "17"
    }
}

dependencies {
    implementation("com.google.firebase:firebase-messaging:24.1.0")
    implementation("com.google.android.gms:play-services-tasks:18.2.0")
    // For NotificationCompat, used by WaveSyncService.startForeground() to
    // build the foreground-service notification. Android requires a
    // notification while a foreground service is running; without one the
    // OS kills the service immediately.
    implementation("androidx.core:core-ktx:1.13.1")
}

tasks.withType<AbstractArchiveTask>().configureEach {
    archiveBaseName.set("wavesync-fcm")
}
