<mxfile host="app.diagrams.net" modified="2021-01-18T06:27:50.561Z" agent="5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36" etag="oOHMG08ZZmhDgFyA8gVO" version="14.2.4" type="github">
  <diagram id="C5RBs43oDa-KdzZeNtuy" name="Page-1">
    <mxGraphModel dx="460" dy="752" grid="1" gridSize="10" guides="1" tooltips="1" connect="1" arrows="1" fold="1" page="1" pageScale="1" pageWidth="827" pageHeight="1169" math="0" shadow="0">
      <root>
        <mxCell id="WIyWlLk6GJQsqaUBKTNV-0" />
        <mxCell id="WIyWlLk6GJQsqaUBKTNV-1" parent="WIyWlLk6GJQsqaUBKTNV-0" />
        <mxCell id="FEzB_F1absWNA4-J2HTH-11" value="" style="rounded=0;html=1;jettySize=auto;orthogonalLoop=1;fontSize=11;endArrow=block;endFill=0;endSize=8;strokeWidth=1;shadow=0;labelBackgroundColor=none;edgeStyle=orthogonalEdgeStyle;" edge="1" source="FEzB_F1absWNA4-J2HTH-12" parent="WIyWlLk6GJQsqaUBKTNV-1">
          <mxGeometry relative="1" as="geometry">
            <mxPoint x="380" y="170" as="targetPoint" />
          </mxGeometry>
        </mxCell>
        <mxCell id="FEzB_F1absWNA4-J2HTH-12" value="Dump image from cvat (SSHOperator)" style="rounded=1;whiteSpace=wrap;html=1;fontSize=12;glass=0;strokeWidth=1;shadow=0;" vertex="1" parent="WIyWlLk6GJQsqaUBKTNV-1">
          <mxGeometry x="320" y="80" width="120" height="40" as="geometry" />
        </mxCell>
        <mxCell id="FEzB_F1absWNA4-J2HTH-13" style="edgeStyle=orthogonalEdgeStyle;rounded=0;orthogonalLoop=1;jettySize=auto;html=1;exitX=0.5;exitY=1;exitDx=0;exitDy=0;" edge="1" source="FEzB_F1absWNA4-J2HTH-14" target="FEzB_F1absWNA4-J2HTH-16" parent="WIyWlLk6GJQsqaUBKTNV-1">
          <mxGeometry relative="1" as="geometry" />
        </mxCell>
        <mxCell id="FEzB_F1absWNA4-J2HTH-14" value="unzip dumped zip files involved images (SSHOperator)" style="rounded=1;whiteSpace=wrap;html=1;fontSize=12;glass=0;strokeWidth=1;shadow=0;" vertex="1" parent="WIyWlLk6GJQsqaUBKTNV-1">
          <mxGeometry x="320" y="170" width="120" height="50" as="geometry" />
        </mxCell>
        <mxCell id="FEzB_F1absWNA4-J2HTH-15" style="edgeStyle=orthogonalEdgeStyle;rounded=0;orthogonalLoop=1;jettySize=auto;html=1;exitX=0.5;exitY=1;exitDx=0;exitDy=0;entryX=0.5;entryY=0;entryDx=0;entryDy=0;" edge="1" source="FEzB_F1absWNA4-J2HTH-16" target="FEzB_F1absWNA4-J2HTH-17" parent="WIyWlLk6GJQsqaUBKTNV-1">
          <mxGeometry relative="1" as="geometry" />
        </mxCell>
        <mxCell id="FEzB_F1absWNA4-J2HTH-16" value="Split images into training &amp;amp; validation (SSHOperator)" style="rounded=1;whiteSpace=wrap;html=1;fontSize=12;glass=0;strokeWidth=1;shadow=0;" vertex="1" parent="WIyWlLk6GJQsqaUBKTNV-1">
          <mxGeometry x="320" y="260" width="120" height="50" as="geometry" />
        </mxCell>
        <mxCell id="FEzB_F1absWNA4-J2HTH-19" style="edgeStyle=orthogonalEdgeStyle;rounded=0;orthogonalLoop=1;jettySize=auto;html=1;exitX=0.5;exitY=1;exitDx=0;exitDy=0;" edge="1" parent="WIyWlLk6GJQsqaUBKTNV-1" source="FEzB_F1absWNA4-J2HTH-17" target="FEzB_F1absWNA4-J2HTH-18">
          <mxGeometry relative="1" as="geometry" />
        </mxCell>
        <mxCell id="FEzB_F1absWNA4-J2HTH-24" style="edgeStyle=orthogonalEdgeStyle;rounded=0;orthogonalLoop=1;jettySize=auto;html=1;" edge="1" parent="WIyWlLk6GJQsqaUBKTNV-1" source="FEzB_F1absWNA4-J2HTH-17" target="FEzB_F1absWNA4-J2HTH-23">
          <mxGeometry relative="1" as="geometry" />
        </mxCell>
        <mxCell id="FEzB_F1absWNA4-J2HTH-17" value="List of training Variables" style="rhombus;whiteSpace=wrap;html=1;" vertex="1" parent="WIyWlLk6GJQsqaUBKTNV-1">
          <mxGeometry x="320" y="350" width="120" height="80" as="geometry" />
        </mxCell>
        <mxCell id="FEzB_F1absWNA4-J2HTH-21" style="edgeStyle=orthogonalEdgeStyle;rounded=0;orthogonalLoop=1;jettySize=auto;html=1;exitX=0.5;exitY=1;exitDx=0;exitDy=0;entryX=0.5;entryY=0;entryDx=0;entryDy=0;" edge="1" parent="WIyWlLk6GJQsqaUBKTNV-1" source="FEzB_F1absWNA4-J2HTH-18" target="FEzB_F1absWNA4-J2HTH-20">
          <mxGeometry relative="1" as="geometry" />
        </mxCell>
        <mxCell id="FEzB_F1absWNA4-J2HTH-18" value="Setting and create yolov5 yamls from templates (SSHOperator)" style="rounded=1;whiteSpace=wrap;html=1;fontSize=12;glass=0;strokeWidth=1;shadow=0;" vertex="1" parent="WIyWlLk6GJQsqaUBKTNV-1">
          <mxGeometry x="320" y="480" width="120" height="70" as="geometry" />
        </mxCell>
        <mxCell id="FEzB_F1absWNA4-J2HTH-22" style="edgeStyle=orthogonalEdgeStyle;rounded=0;orthogonalLoop=1;jettySize=auto;html=1;exitX=0;exitY=0.5;exitDx=0;exitDy=0;entryX=0;entryY=0.5;entryDx=0;entryDy=0;" edge="1" parent="WIyWlLk6GJQsqaUBKTNV-1" source="FEzB_F1absWNA4-J2HTH-20" target="FEzB_F1absWNA4-J2HTH-17">
          <mxGeometry relative="1" as="geometry">
            <Array as="points">
              <mxPoint x="230" y="635" />
              <mxPoint x="230" y="390" />
            </Array>
          </mxGeometry>
        </mxCell>
        <mxCell id="FEzB_F1absWNA4-J2HTH-20" value="Start train with docker (SSHOperator)" style="rounded=1;whiteSpace=wrap;html=1;fontSize=12;glass=0;strokeWidth=1;shadow=0;" vertex="1" parent="WIyWlLk6GJQsqaUBKTNV-1">
          <mxGeometry x="320" y="600" width="120" height="70" as="geometry" />
        </mxCell>
        <mxCell id="FEzB_F1absWNA4-J2HTH-23" value="Stop train" style="rounded=1;whiteSpace=wrap;html=1;fontSize=12;glass=0;strokeWidth=1;shadow=0;" vertex="1" parent="WIyWlLk6GJQsqaUBKTNV-1">
          <mxGeometry x="490" y="362.5" width="120" height="55" as="geometry" />
        </mxCell>
      </root>
    </mxGraphModel>
  </diagram>
</mxfile>
