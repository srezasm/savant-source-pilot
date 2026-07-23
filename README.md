# Savant Source Pilot

Savant Source Pilot manages RTSP sources for [Savant](https://github.com/insight-platform/Savant) pipelines dynamically. It listens for source add/remove events on Kafka, validates the stream URL, and automatically starts or stops the matching Docker adapter container — so you don't have to manually spin up or tear down adapters every time a camera comes online or drops off.

Savant already supports attaching and detaching RTSP sources on the fly without restarting the pipeline. Source Pilot builds on that capability, turning source management into an event-driven process instead of a manual one, and making it easier to run pipelines with many dynamic sources at scale.

> **Status:** This project is under active development.
